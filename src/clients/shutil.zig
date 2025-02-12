const builtin = @import("builtin");
const std = @import("std");
const assert = std.debug.assert;

pub const command_separator = if (builtin.os.tag == .windows) ";" else "&&";
pub const path_separator = if (builtin.os.tag == .windows) "\\" else "/";

pub fn exec(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
) !std.ChildProcess.ExecResult {
    var res = try std.ChildProcess.exec(.{
        .allocator = arena.allocator(),
        .argv = cmd,
    });
    switch (res.term) {
        .Exited => |code| {
            if (code != 0) {
                return error.ExecCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }

    return res;
}

pub fn run_with_env(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
    env: []const []const u8,
) !void {
    std.debug.print("Running:", .{});
    var i: u32 = 0;
    while (i < env.len) : (i += 2) {
        std.debug.print(" {s}={s}", .{ env[i], env[i + 1] });
    }
    for (cmd) |c| {
        std.debug.print(" {s}", .{c});
    }
    std.debug.print("\n", .{});

    var cp = std.ChildProcess.init(cmd, arena.allocator());

    var env_map = try std.process.getEnvMap(arena.allocator());
    i = 0;
    while (i < env.len) : (i += 2) {
        try env_map.put(env[i], env[i + 1]);
    }
    cp.env_map = &env_map;

    var res = try cp.spawnAndWait();
    switch (res) {
        .Exited => |code| {
            if (code != 0) {
                std.debug.print("Failed to run:", .{});
                i = 0;
                while (i < env.len) : (i += 2) {
                    std.debug.print(" {s}={s}", .{ env[i], env[i + 1] });
                }
                for (cmd) |c| {
                    std.debug.print(" {s}", .{c});
                }
                std.debug.print("\n", .{});
                return error.RunCommandFailed;
            }
        },

        else => return error.UnknownCase,
    }
}

// Needs to have -e for `sh` otherwise the command might not fail.
pub fn shell_wrap(arena: *std.heap.ArenaAllocator, cmd: []const u8) ![]const []const u8 {
    var wrapped = std.ArrayList([]const u8).init(arena.allocator());
    if (builtin.os.tag == .windows) {
        try wrapped.append("powershell");
    } else {
        try wrapped.append("sh");
        try wrapped.append("-e");
    }
    try wrapped.append("-c");
    if (builtin.os.tag == .windows) {
        try wrapped.append(try std.fmt.allocPrint(
            arena.allocator(),
            \\Set-PSDebug -Trace 1
            \\
            \\{s}
        ,
            .{try std.mem.replaceOwned(
                u8,
                arena.allocator(),
                cmd,
                ";",
                "; if(!$?) { Exit $LASTEXITCODE }; ",
            )},
        ));
    } else {
        try wrapped.append(cmd);
    }
    return wrapped.items;
}

pub fn run_shell_with_env(
    arena: *std.heap.ArenaAllocator,
    cmd: []const u8,
    env: []const []const u8,
) !void {
    try run_with_env(
        arena,
        try shell_wrap(arena, cmd),
        env,
    );
}

pub fn run(
    arena: *std.heap.ArenaAllocator,
    cmd: []const []const u8,
) !void {
    try run_with_env(arena, cmd, &.{});
}

pub fn run_shell(
    arena: *std.heap.ArenaAllocator,
    cmd: []const u8,
) !void {
    try run_shell_with_env(arena, cmd, &.{});
}

pub const TmpDir = struct {
    dir: std.testing.TmpDir,
    path: []const u8,

    pub fn init(arena: *std.heap.ArenaAllocator) !TmpDir {
        var tmp_dir = std.testing.tmpDir(.{});
        return TmpDir{
            .dir = tmp_dir,
            .path = try tmp_dir.dir.realpathAlloc(arena.allocator(), "."),
        };
    }

    pub fn deinit(self: *TmpDir) void {
        self.dir.cleanup();
    }
};

pub fn git_root(arena: *std.heap.ArenaAllocator) ![]const u8 {
    const exec_result = try exec(arena, &.{ "git", "rev-parse", "--show-toplevel" });
    // As conventional, output includes a trailing `\n` which we need to strip away.
    assert(!std.mem.endsWith(u8, exec_result.stdout, "\r\n"));
    assert(std.mem.endsWith(u8, exec_result.stdout, "\n"));
    return exec_result.stdout[0 .. exec_result.stdout.len - 1];
}

// Makes sure a local script name has the platform-appropriate folder
// character and extension. This is particularly important in Windows
// when you are calling a program. arg0 must be a well-formed path
// otherwise you have to pass it through powershell -c '...' and that's a
// waste.
pub fn script_filename(arena: *std.heap.ArenaAllocator, parts: []const []const u8) ![]const u8 {
    var file_name = std.ArrayList(u8).init(arena.allocator());

    _ = try file_name.append('.');
    for (parts) |part| {
        _ = try file_name.appendSlice(path_separator);
        _ = try file_name.appendSlice(part);
    }

    _ = try file_name.appendSlice(if (builtin.os.tag == .windows) ".bat" else ".sh");
    return file_name.items;
}

pub fn binary_filename(arena: *std.heap.ArenaAllocator, parts: []const []const u8) ![]const u8 {
    var file_name = std.ArrayList(u8).init(arena.allocator());
    _ = try file_name.append('.');
    for (parts) |part| {
        _ = try file_name.appendSlice(path_separator);
        _ = try file_name.appendSlice(part);
    }

    _ = try file_name.appendSlice(if (builtin.os.tag == .windows) ".exe" else "");
    return file_name.items;
}

pub fn file_or_directory_exists(f_or_d: []const u8) bool {
    var file = std.fs.cwd().openFile(f_or_d, .{}) catch |err| {
        switch (err) {
            error.FileNotFound => return false,
            error.IsDir => return true,
            else => std.debug.panic(
                "unexpected error while checking file_or_directory_exists({s}): {s}",
                .{ f_or_d, @errorName(err) },
            ),
        }
    };
    file.close();

    return true;
}

pub fn write_shell_newlines_into_single_line(
    into: *std.ArrayList(u8),
    from: []const u8,
) !void {
    if (from.len == 0) {
        return;
    }

    var lines = std.mem.split(u8, from, "\n");
    while (lines.next()) |line| {
        try into.writer().print("{s} {s} ", .{ line, command_separator });
    }

    // The above commands all end with ` {command_separator} `
    try into.appendSlice("echo ok");
}
