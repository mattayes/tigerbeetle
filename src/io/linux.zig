const std = @import("std");
const assert = std.debug.assert;
const os = std.os;
const linux = os.linux;
const IO_Uring = linux.IO_Uring;
const io_uring_cqe = linux.io_uring_cqe;
const io_uring_sqe = linux.io_uring_sqe;
const log = std.log.scoped(.io);
const tracer = @import("../tracer.zig");

const constants = @import("../constants.zig");
const stdx = @import("../stdx.zig");
const FIFO = @import("../fifo.zig").FIFO;
const buffer_limit = @import("../io.zig").buffer_limit;
const parse_dirty_semver = stdx.parse_dirty_semver;

pub const IO = struct {
    ring: IO_Uring,

    /// Operations not yet submitted to the kernel and waiting on available space in the
    /// submission queue.
    unqueued: FIFO(Completion) = .{ .name = "io_unqueued" },

    /// Completions that are ready to have their callbacks run.
    completed: FIFO(Completion) = .{ .name = "io_completed" },

    ios_queued: u64 = 0,
    ios_in_kernel: u64 = 0,

    flush_tracer_slot: ?tracer.SpanStart = null,
    callback_tracer_slot: ?tracer.SpanStart = null,

    pub fn init(entries: u12, flags: u32) !IO {
        // Detect the linux version to ensure that we support all io_uring ops used.
        const uts = std.os.uname();
        const version = try parse_dirty_semver(&uts.release);
        if (version.order(std.SemanticVersion{ .major = 5, .minor = 5, .patch = 0 }) == .lt) {
            @panic("Linux kernel 5.5 or greater is required for io_uring OP_ACCEPT");
        }

        return IO{ .ring = try IO_Uring.init(entries, flags) };
    }

    pub fn deinit(self: *IO) void {
        assert(self.flush_tracer_slot == null);
        assert(self.callback_tracer_slot == null);

        self.ring.deinit();
    }

    /// Pass all queued submissions to the kernel and peek for completions.
    pub fn tick(self: *IO) !void {
        // We assume that all timeouts submitted by `run_for_ns()` will be reaped by `run_for_ns()`
        // and that `tick()` and `run_for_ns()` cannot be run concurrently.
        // Therefore `timeouts` here will never be decremented and `etime` will always be false.
        var timeouts: usize = 0;
        var etime = false;

        try self.flush(0, &timeouts, &etime);
        assert(etime == false);

        // Flush any SQEs that were queued while running completion callbacks in `flush()`:
        // This is an optimization to avoid delaying submissions until the next tick.
        // At the same time, we do not flush any ready CQEs since SQEs may complete synchronously.
        // We guard against an io_uring_enter() syscall if we know we do not have any queued SQEs.
        // We cannot use `self.ring.sq_ready()` here since this counts flushed and unflushed SQEs.
        const queued = self.ring.sq.sqe_tail -% self.ring.sq.sqe_head;
        if (queued > 0) {
            try self.flush_submissions(0, &timeouts, &etime);
            assert(etime == false);
        }
    }

    /// Pass all queued submissions to the kernel and run for `nanoseconds`.
    /// The `nanoseconds` argument is a u63 to allow coercion to the i64 used
    /// in the kernel_timespec struct.
    pub fn run_for_ns(self: *IO, nanoseconds: u63) !void {
        // We must use the same clock source used by io_uring (CLOCK_MONOTONIC) since we specify the
        // timeout below as an absolute value. Otherwise, we may deadlock if the clock sources are
        // dramatically different. Any kernel that supports io_uring will support CLOCK_MONOTONIC.
        var current_ts: os.timespec = undefined;
        os.clock_gettime(os.CLOCK.MONOTONIC, &current_ts) catch unreachable;
        // The absolute CLOCK_MONOTONIC time after which we may return from this function:
        const timeout_ts: os.linux.kernel_timespec = .{
            .tv_sec = current_ts.tv_sec,
            .tv_nsec = current_ts.tv_nsec + nanoseconds,
        };
        var timeouts: usize = 0;
        var etime = false;
        while (!etime) {
            const timeout_sqe = self.ring.get_sqe() catch blk: {
                // The submission queue is full, so flush submissions to make space:
                try self.flush_submissions(0, &timeouts, &etime);
                break :blk self.ring.get_sqe() catch unreachable;
            };
            // Submit an absolute timeout that will be canceled if any other SQE completes first:
            linux.io_uring_prep_timeout(timeout_sqe, &timeout_ts, 1, os.linux.IORING_TIMEOUT_ABS);
            timeout_sqe.user_data = 0;
            timeouts += 1;

            // We don't really want to count this timeout as an io,
            // but it's tricky to track separately.
            self.ios_queued += 1;
            tracer.plot(
                .{ .queue_count = .{ .queue_name = "io_queued" } },
                @as(f64, @floatFromInt(self.ios_queued)),
            );

            // The amount of time this call will block is bounded by the timeout we just submitted:
            try self.flush(1, &timeouts, &etime);
        }
        // Reap any remaining timeouts, which reference the timespec in the current stack frame.
        // The busy loop here is required to avoid a potential deadlock, as the kernel determines
        // when the timeouts are pushed to the completion queue, not us.
        while (timeouts > 0) _ = try self.flush_completions(0, &timeouts, &etime);
    }

    fn flush(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        tracer.start(
            &self.flush_tracer_slot,
            .io_flush,
            @src(),
        );

        // Flush any queued SQEs and reuse the same syscall to wait for completions if required:
        try self.flush_submissions(wait_nr, timeouts, etime);
        // We can now just peek for any CQEs without waiting and without another syscall:
        try self.flush_completions(0, timeouts, etime);

        // The SQE array is empty from flush_submissions(). Fill it up with unqueued completions.
        // This runs before `self.completed` is flushed below to prevent new IO from reserving SQE
        // slots and potentially starving those in `self.unqueued`.
        // Loop over a copy to avoid an infinite loop of `enqueue()` re-adding to `self.unqueued`.
        {
            var copy = self.unqueued;
            self.unqueued.reset();
            while (copy.pop()) |completion| self.enqueue(completion);
        }

        tracer.end(
            &self.flush_tracer_slot,
            .io_flush,
        );

        // Run completions only after all completions have been flushed:
        // Loop until all completions are processed. Calls to complete() may queue more work
        // and extend the duration of the loop, but this is fine as it 1) executes completions
        // that become ready without going through another syscall from flush_submissions() and
        // 2) potentially queues more SQEs to take advantage more of the next flush_submissions().
        while (self.completed.pop()) |completion| completion.complete(&self.callback_tracer_slot);

        // At this point, unqueued could have completions either by 1) those who didn't get an SQE
        // during the popping of unqueued or 2) completion.complete() which start new IO. These
        // unqueued completions will get priority to acquiring SQEs on the next flush().
    }

    fn flush_completions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        var cqes: [256]io_uring_cqe = undefined;
        var wait_remaining = wait_nr;
        while (true) {
            // Guard against waiting indefinitely (if there are too few requests inflight),
            // especially if this is not the first time round the loop:
            const completed = self.ring.copy_cqes(&cqes, wait_remaining) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                else => return err,
            };
            if (completed > wait_remaining) wait_remaining = 0 else wait_remaining -= completed;
            for (cqes[0..completed]) |cqe| {
                self.ios_in_kernel -= 1;

                if (cqe.user_data == 0) {
                    timeouts.* -= 1;
                    // We are only done if the timeout submitted was completed due to time, not if
                    // it was completed due to the completion of an event, in which case `cqe.res`
                    // would be 0. It is possible for multiple timeout operations to complete at the
                    // same time if the nanoseconds value passed to `run_for_ns()` is very short.
                    if (-cqe.res == @intFromEnum(os.E.TIME)) etime.* = true;
                    continue;
                }
                const completion = @as(*Completion, @ptrFromInt(@as(usize, @intCast(cqe.user_data))));
                completion.result = cqe.res;
                // We do not run the completion here (instead appending to a linked list) to avoid:
                // * recursion through `flush_submissions()` and `flush_completions()`,
                // * unbounded stack usage, and
                // * confusing stack traces.
                self.completed.push(completion);
            }

            tracer.plot(
                .{ .queue_count = .{ .queue_name = "io_in_kernel" } },
                @as(f64, @floatFromInt(self.ios_in_kernel)),
            );

            if (completed < cqes.len) break;
        }
    }

    fn flush_submissions(self: *IO, wait_nr: u32, timeouts: *usize, etime: *bool) !void {
        while (true) {
            const submitted = self.ring.submit_and_wait(wait_nr) catch |err| switch (err) {
                error.SignalInterrupt => continue,
                // Wait for some completions and then try again:
                // See https://github.com/axboe/liburing/issues/281 re: error.SystemResources.
                // Be careful also that copy_cqes() will flush before entering to wait (it does):
                // https://github.com/axboe/liburing/commit/35c199c48dfd54ad46b96e386882e7ac341314c5
                error.CompletionQueueOvercommitted, error.SystemResources => {
                    try self.flush_completions(1, timeouts, etime);
                    continue;
                },
                else => return err,
            };

            self.ios_queued -= submitted;
            self.ios_in_kernel += submitted;
            tracer.plot(
                .{ .queue_count = .{ .queue_name = "io_queued" } },
                @as(f64, @floatFromInt(self.ios_queued)),
            );
            tracer.plot(
                .{ .queue_count = .{ .queue_name = "io_in_kernel" } },
                @as(f64, @floatFromInt(self.ios_in_kernel)),
            );

            break;
        }
    }

    fn enqueue(self: *IO, completion: *Completion) void {
        const sqe = self.ring.get_sqe() catch |err| switch (err) {
            error.SubmissionQueueFull => {
                self.unqueued.push(completion);
                return;
            },
        };
        completion.prep(sqe);

        self.ios_queued += 1;
        tracer.plot(
            .{ .queue_count = .{ .queue_name = "io_queued" } },
            @as(f64, @floatFromInt(self.ios_queued)),
        );
    }

    /// This struct holds the data needed for a single io_uring operation
    pub const Completion = struct {
        io: *IO,
        result: i32 = undefined,
        next: ?*Completion = null,
        operation: Operation,
        context: ?*anyopaque,
        callback: *const fn (context: ?*anyopaque, completion: *Completion, result: *const anyopaque) void,

        fn prep(completion: *Completion, sqe: *io_uring_sqe) void {
            switch (completion.operation) {
                .accept => |*op| {
                    linux.io_uring_prep_accept(
                        sqe,
                        op.socket,
                        &op.address,
                        &op.address_size,
                        os.SOCK.CLOEXEC,
                    );
                },
                .close => |op| {
                    linux.io_uring_prep_close(sqe, op.fd);
                },
                .connect => |*op| {
                    linux.io_uring_prep_connect(
                        sqe,
                        op.socket,
                        &op.address.any,
                        op.address.getOsSockLen(),
                    );
                },
                .read => |op| {
                    linux.io_uring_prep_read(
                        sqe,
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
                .recv => |op| {
                    linux.io_uring_prep_recv(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
                },
                .send => |op| {
                    linux.io_uring_prep_send(sqe, op.socket, op.buffer, os.MSG.NOSIGNAL);
                },
                .timeout => |*op| {
                    linux.io_uring_prep_timeout(sqe, &op.timespec, 0, 0);
                },
                .write => |op| {
                    linux.io_uring_prep_write(
                        sqe,
                        op.fd,
                        op.buffer[0..buffer_limit(op.buffer.len)],
                        op.offset,
                    );
                },
            }
            sqe.user_data = @intFromPtr(completion);
        }

        fn complete(completion: *Completion, callback_tracer_slot: *?tracer.SpanStart) void {
            switch (completion.operation) {
                .accept => {
                    const result: anyerror!os.socket_t = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNABORTED => error.ConnectionAborted,
                                .FAULT => unreachable,
                                .INVAL => error.SocketNotListening,
                                .MFILE => error.ProcessFdQuotaExceeded,
                                .NFILE => error.SystemFdQuotaExceeded,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PERM => error.PermissionDenied,
                                .PROTO => error.ProtocolFailure,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @as(os.socket_t, @intCast(completion.result));
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .close => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {}, // A success, see https://github.com/ziglang/zig/issues/2425
                                .BADF => error.FileDescriptorInvalid,
                                .DQUOT => error.DiskQuota,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .connect => {
                    const result: anyerror!void = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .ADDRINUSE => error.AddressInUse,
                                .ADDRNOTAVAIL => error.AddressNotAvailable,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .AGAIN, .INPROGRESS => error.WouldBlock,
                                .ALREADY => error.OpenAlreadyInProgress,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .ISCONN => error.AlreadyConnected,
                                .NETUNREACH => error.NetworkUnreachable,
                                .NOENT => error.FileNotFound,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .PERM => error.PermissionDenied,
                                .PROTOTYPE => error.ProtocolNotSupported,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            assert(completion.result == 0);
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .read => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForReading,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .FAULT => unreachable,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .ISDIR => error.IsDir,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .SPIPE => error.Unseekable,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @as(usize, @intCast(completion.result));
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .recv => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNREFUSED => error.ConnectionRefused,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                .OPNOTSUPP => error.OperationNotSupported,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @as(usize, @intCast(completion.result));
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .send => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .ACCES => error.AccessDenied,
                                .AGAIN => error.WouldBlock,
                                .ALREADY => error.FastOpenAlreadyInProgress,
                                .AFNOSUPPORT => error.AddressFamilyNotSupported,
                                .BADF => error.FileDescriptorInvalid,
                                .CONNRESET => error.ConnectionResetByPeer,
                                .DESTADDRREQ => unreachable,
                                .FAULT => unreachable,
                                .INVAL => unreachable,
                                .ISCONN => unreachable,
                                .MSGSIZE => error.MessageTooBig,
                                .NOBUFS => error.SystemResources,
                                .NOMEM => error.SystemResources,
                                .NOTCONN => error.SocketNotConnected,
                                .NOTSOCK => error.FileDescriptorNotASocket,
                                .OPNOTSUPP => error.OperationNotSupported,
                                .PIPE => error.BrokenPipe,
                                .TIMEDOUT => error.ConnectionTimedOut,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @as(usize, @intCast(completion.result));
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .timeout => {
                    assert(completion.result < 0);
                    const result: anyerror!void = switch (@as(os.E, @enumFromInt(-completion.result))) {
                        .INTR => {
                            completion.io.enqueue(completion);
                            return;
                        },
                        .CANCELED => error.Canceled,
                        .TIME => {}, // A success.
                        else => |errno| os.unexpectedErrno(errno),
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
                .write => {
                    const result: anyerror!usize = blk: {
                        if (completion.result < 0) {
                            const err = switch (@as(os.E, @enumFromInt(-completion.result))) {
                                .INTR => {
                                    completion.io.enqueue(completion);
                                    return;
                                },
                                .AGAIN => error.WouldBlock,
                                .BADF => error.NotOpenForWriting,
                                .DESTADDRREQ => error.NotConnected,
                                .DQUOT => error.DiskQuota,
                                .FAULT => unreachable,
                                .FBIG => error.FileTooBig,
                                .INVAL => error.Alignment,
                                .IO => error.InputOutput,
                                .NOSPC => error.NoSpaceLeft,
                                .NXIO => error.Unseekable,
                                .OVERFLOW => error.Unseekable,
                                .PERM => error.AccessDenied,
                                .PIPE => error.BrokenPipe,
                                .SPIPE => error.Unseekable,
                                else => |errno| os.unexpectedErrno(errno),
                            };
                            break :blk err;
                        } else {
                            break :blk @as(usize, @intCast(completion.result));
                        }
                    };
                    call_callback(completion, &result, callback_tracer_slot);
                },
            }
        }
    };

    fn call_callback(
        completion: *Completion,
        result: *const anyopaque,
        callback_tracer_slot: *?tracer.SpanStart,
    ) void {
        tracer.start(
            callback_tracer_slot,
            .io_callback,
            @src(),
        );
        completion.callback(completion.context, completion, result);
        tracer.end(
            callback_tracer_slot,
            .io_callback,
        );
    }

    /// This union encodes the set of operations supported as well as their arguments.
    const Operation = union(enum) {
        accept: struct {
            socket: os.socket_t,
            address: os.sockaddr = undefined,
            address_size: os.socklen_t = @sizeOf(os.sockaddr),
        },
        close: struct {
            fd: os.fd_t,
        },
        connect: struct {
            socket: os.socket_t,
            address: std.net.Address,
        },
        read: struct {
            fd: os.fd_t,
            buffer: []u8,
            offset: u64,
        },
        recv: struct {
            socket: os.socket_t,
            buffer: []u8,
        },
        send: struct {
            socket: os.socket_t,
            buffer: []const u8,
        },
        timeout: struct {
            timespec: os.linux.kernel_timespec,
        },
        write: struct {
            fd: os.fd_t,
            buffer: []const u8,
            offset: u64,
        },
    };

    pub const AcceptError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionAborted,
        SocketNotListening,
        ProcessFdQuotaExceeded,
        SystemFdQuotaExceeded,
        SystemResources,
        FileDescriptorNotASocket,
        OperationNotSupported,
        PermissionDenied,
        ProtocolFailure,
    } || os.UnexpectedError;

    pub fn accept(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: AcceptError!os.socket_t,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const AcceptError!os.socket_t, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .accept = .{
                    .socket = socket,
                    .address = undefined,
                    .address_size = @sizeOf(os.sockaddr),
                },
            },
        };
        self.enqueue(completion);
    }

    pub const CloseError = error{
        FileDescriptorInvalid,
        DiskQuota,
        InputOutput,
        NoSpaceLeft,
    } || os.UnexpectedError;

    pub fn close(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: CloseError!void,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const CloseError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .close = .{ .fd = fd },
            },
        };
        self.enqueue(completion);
    }

    pub const ConnectError = error{
        AccessDenied,
        AddressInUse,
        AddressNotAvailable,
        AddressFamilyNotSupported,
        WouldBlock,
        OpenAlreadyInProgress,
        FileDescriptorInvalid,
        ConnectionRefused,
        AlreadyConnected,
        NetworkUnreachable,
        FileNotFound,
        FileDescriptorNotASocket,
        PermissionDenied,
        ProtocolNotSupported,
        ConnectionTimedOut,
        SystemResources,
    } || os.UnexpectedError;

    pub fn connect(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ConnectError!void,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        address: std.net.Address,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ConnectError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .connect = .{
                    .socket = socket,
                    .address = address,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const ReadError = error{
        WouldBlock,
        NotOpenForReading,
        ConnectionResetByPeer,
        Alignment,
        InputOutput,
        IsDir,
        SystemResources,
        Unseekable,
        ConnectionTimedOut,
    } || os.UnexpectedError;

    pub fn read(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: ReadError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const ReadError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .read = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const RecvError = error{
        WouldBlock,
        FileDescriptorInvalid,
        ConnectionRefused,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        ConnectionTimedOut,
        OperationNotSupported,
    } || os.UnexpectedError;

    pub fn recv(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: RecvError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const RecvError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .recv = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const SendError = error{
        AccessDenied,
        WouldBlock,
        FastOpenAlreadyInProgress,
        AddressFamilyNotSupported,
        FileDescriptorInvalid,
        ConnectionResetByPeer,
        MessageTooBig,
        SystemResources,
        SocketNotConnected,
        FileDescriptorNotASocket,
        OperationNotSupported,
        BrokenPipe,
        ConnectionTimedOut,
    } || os.UnexpectedError;

    pub fn send(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: SendError!usize,
        ) void,
        completion: *Completion,
        socket: os.socket_t,
        buffer: []const u8,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const SendError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .send = .{
                    .socket = socket,
                    .buffer = buffer,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const TimeoutError = error{Canceled} || os.UnexpectedError;

    pub fn timeout(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: TimeoutError!void,
        ) void,
        completion: *Completion,
        nanoseconds: u63,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const TimeoutError!void, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .timeout = .{
                    .timespec = .{ .tv_sec = 0, .tv_nsec = nanoseconds },
                },
            },
        };

        // Special case a zero timeout as a yield.
        if (nanoseconds == 0) {
            completion.result = -@as(i32, @intCast(@intFromEnum(std.os.E.TIME)));
            self.completed.push(completion);
            return;
        }

        self.enqueue(completion);
    }

    pub const WriteError = error{
        WouldBlock,
        NotOpenForWriting,
        NotConnected,
        DiskQuota,
        FileTooBig,
        Alignment,
        InputOutput,
        NoSpaceLeft,
        Unseekable,
        AccessDenied,
        BrokenPipe,
    } || os.UnexpectedError;

    pub fn write(
        self: *IO,
        comptime Context: type,
        context: Context,
        comptime callback: fn (
            context: Context,
            completion: *Completion,
            result: WriteError!usize,
        ) void,
        completion: *Completion,
        fd: os.fd_t,
        buffer: []const u8,
        offset: u64,
    ) void {
        completion.* = .{
            .io = self,
            .context = context,
            .callback = struct {
                fn wrapper(ctx: ?*anyopaque, comp: *Completion, res: *const anyopaque) void {
                    callback(
                        @ptrCast(@alignCast(ctx)),
                        comp,
                        @as(*const WriteError!usize, @ptrCast(@alignCast(res))).*,
                    );
                }
            }.wrapper,
            .operation = .{
                .write = .{
                    .fd = fd,
                    .buffer = buffer,
                    .offset = offset,
                },
            },
        };
        self.enqueue(completion);
    }

    pub const INVALID_SOCKET = -1;

    /// Creates a socket that can be used for async operations with the IO instance.
    pub fn open_socket(self: *IO, family: u32, sock_type: u32, protocol: u32) !os.socket_t {
        _ = self;
        return os.socket(family, sock_type, protocol);
    }

    /// Opens a directory with read only access.
    pub fn open_dir(dir_path: []const u8) !os.fd_t {
        return os.open(dir_path, os.O.CLOEXEC | os.O.RDONLY, 0);
    }

    pub const INVALID_FILE: os.fd_t = -1;

    /// Opens or creates a journal file:
    /// - For reading and writing.
    /// - For Direct I/O (if possible in development mode, but required in production mode).
    /// - Obtains an advisory exclusive lock to the file descriptor.
    /// - Allocates the file contiguously on disk if this is supported by the file system.
    /// - Ensures that the file data (and file inode in the parent directory) is durable on disk.
    ///   The caller is responsible for ensuring that the parent directory inode is durable.
    /// - Verifies that the file size matches the expected file size before returning.
    pub fn open_file(
        dir_fd: os.fd_t,
        relative_path: []const u8,
        size: u64,
        method: enum { create, create_or_open, open },
    ) !os.fd_t {
        assert(relative_path.len > 0);
        assert(size % constants.sector_size == 0);

        // TODO Use O_EXCL when opening as a block device to obtain a mandatory exclusive lock.
        // This is much stronger than an advisory exclusive lock, and is required on some platforms.

        var flags: u32 = os.O.CLOEXEC | os.O.RDWR | os.O.DSYNC;
        var mode: os.mode_t = 0;

        // TODO Document this and investigate whether this is in fact correct to set here.
        if (@hasDecl(os.O, "LARGEFILE")) flags |= os.O.LARGEFILE;

        var direct_io_supported = false;
        var dir_on_tmpfs = try fs_is_tmpfs(dir_fd);

        if (dir_on_tmpfs) {
            log.warn("tmpfs is not durable, and your data will be lost on reboot", .{});
        }

        // Special case. tmpfs doesn't support Direct I/O. Normally we would panic here (see below)
        // but being able to benchmark production workloads on tmpfs is very useful for removing
        // disk speed from the equation.
        if (constants.direct_io and !dir_on_tmpfs) {
            direct_io_supported = try fs_supports_direct_io(dir_fd);
            if (direct_io_supported) {
                flags |= os.O.DIRECT;
            } else if (!constants.direct_io_required) {
                log.warn("file system does not support Direct I/O", .{});
            } else {
                // We require Direct I/O for safety to handle fsync failure correctly, and therefore
                // panic in production if it is not supported.
                @panic("file system does not support Direct I/O");
            }
        }

        switch (method) {
            .create => {
                flags |= os.O.CREAT;
                flags |= os.O.EXCL;
                mode = 0o666;
                log.info("creating \"{s}\"...", .{relative_path});
            },
            .create_or_open => {
                flags |= os.O.CREAT;
                mode = 0o666;
                log.info("opening or creating \"{s}\"...", .{relative_path});
            },
            .open => {
                log.info("opening \"{s}\"...", .{relative_path});
            },
        }

        // This is critical as we rely on O_DSYNC for fsync() whenever we write to the file:
        assert((flags & os.O.DSYNC) > 0);

        // Be careful with openat(2): "If pathname is absolute, then dirfd is ignored." (man page)
        assert(!std.fs.path.isAbsolute(relative_path));
        const fd = try os.openat(dir_fd, relative_path, flags, mode);
        // TODO Return a proper error message when the path exists or does not exist (init/start).
        errdefer os.close(fd);

        // TODO Check that the file is actually a file.

        // Obtain an advisory exclusive lock that works only if all processes actually use flock().
        // LOCK_NB means that we want to fail the lock without waiting if another process has it.
        os.flock(fd, os.LOCK.EX | os.LOCK.NB) catch |err| switch (err) {
            error.WouldBlock => @panic("another process holds the data file lock"),
            else => return err,
        };

        // Ask the file system to allocate contiguous sectors for the file (if possible):
        // If the file system does not support `fallocate()`, then this could mean more seeks or a
        // panic if we run out of disk space (ENOSPC).
        if (method == .create) {
            log.info("allocating {}...", .{std.fmt.fmtIntSizeBin(size)});
            fs_allocate(fd, size) catch |err| switch (err) {
                error.OperationNotSupported => {
                    log.warn("file system does not support fallocate(), an ENOSPC will panic", .{});
                    log.info("allocating by writing to the last sector of the file instead...", .{});

                    const sector_size = constants.sector_size;
                    const sector: [sector_size]u8 align(sector_size) = [_]u8{0} ** sector_size;

                    // Handle partial writes where the physical sector is less than a logical sector:
                    const write_offset = size - sector.len;
                    var written: usize = 0;
                    while (written < sector.len) {
                        written += try os.pwrite(fd, sector[written..], write_offset + written);
                    }
                },
                else => |e| return e,
            };
        }

        // The best fsync strategy is always to fsync before reading because this prevents us from
        // making decisions on data that was never durably written by a previously crashed process.
        // We therefore always fsync when we open the path, also to wait for any pending O_DSYNC.
        // Thanks to Alex Miller from FoundationDB for diving into our source and pointing this out.
        try os.fsync(fd);

        // We fsync the parent directory to ensure that the file inode is durably written.
        // The caller is responsible for the parent directory inode stored under the grandparent.
        // We always do this when opening because we don't know if this was done before crashing.
        try os.fsync(dir_fd);

        const stat = try os.fstat(fd);
        if (stat.size < size) @panic("data file inode size was truncated or corrupted");

        return fd;
    }

    /// Detects whether the underlying file system for a given directory fd is tmpfs. This is used
    /// to relax our Direct I/O check - running on tmpfs for benchmarking is useful.
    fn fs_is_tmpfs(dir_fd: std.os.fd_t) !bool {
        var statfs: stdx.StatFs = undefined;

        while (true) {
            const res = stdx.fstatfs(dir_fd, &statfs);
            switch (os.linux.getErrno(res)) {
                .SUCCESS => {
                    return statfs.f_type == stdx.TmpfsMagic;
                },
                .INTR => continue,
                else => |err| return os.unexpectedErrno(err),
            }
        }
    }

    /// Detects whether the underlying file system for a given directory fd supports Direct I/O.
    /// Not all Linux file systems support `O_DIRECT`, e.g. a shared macOS volume.
    fn fs_supports_direct_io(dir_fd: std.os.fd_t) !bool {
        if (!@hasDecl(std.os.O, "DIRECT")) return false;

        const path = "fs_supports_direct_io";
        const dir = std.fs.Dir{ .fd = dir_fd };
        const fd = try os.openatZ(dir_fd, path, os.O.CLOEXEC | os.O.CREAT | os.O.TRUNC, 0o666);
        defer os.close(fd);
        defer dir.deleteFile(path) catch {};

        while (true) {
            const res = os.linux.openat(dir_fd, path, os.O.CLOEXEC | os.O.RDONLY | os.O.DIRECT, 0);
            switch (os.linux.getErrno(res)) {
                .SUCCESS => {
                    os.close(@as(os.fd_t, @intCast(res)));
                    return true;
                },
                .INTR => continue,
                .INVAL => return false,
                else => |err| return os.unexpectedErrno(err),
            }
        }
    }

    /// Allocates a file contiguously using fallocate() if supported.
    /// Alternatively, writes to the last sector so that at least the file size is correct.
    fn fs_allocate(fd: os.fd_t, size: u64) !void {
        const mode: i32 = 0;
        const offset: i64 = 0;
        const length = @as(i64, @intCast(size));

        while (true) {
            const rc = os.linux.fallocate(fd, mode, offset, length);
            switch (os.linux.getErrno(rc)) {
                .SUCCESS => return,
                .BADF => return error.FileDescriptorInvalid,
                .FBIG => return error.FileTooBig,
                .INTR => continue,
                .INVAL => return error.ArgumentsInvalid,
                .IO => return error.InputOutput,
                .NODEV => return error.NoDevice,
                .NOSPC => return error.NoSpaceLeft,
                .NOSYS => return error.SystemOutdated,
                .OPNOTSUPP => return error.OperationNotSupported,
                .PERM => return error.PermissionDenied,
                .SPIPE => return error.Unseekable,
                .TXTBSY => return error.FileBusy,
                else => |errno| return os.unexpectedErrno(errno),
            }
        }
    }
};
