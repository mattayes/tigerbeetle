const std = @import("std");
const tb = @import("../../tigerbeetle.zig");
const tb_client = @import("../c/tb_client.zig");

const trait = std.meta.trait;
const assert = std.debug.assert;

const output_path = "src/clients/java/src/main/java/com/tigerbeetle/";

const TypeMapping = struct {
    name: []const u8,
    private_fields: []const []const u8 = &.{},
    readonly_fields: []const []const u8 = &.{},
    docs_link: ?[]const u8 = null,
    visibility: enum { public, internal } = .public,

    pub fn is_private(comptime self: @This(), name: []const u8) bool {
        inline for (self.private_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }

    pub fn is_read_only(comptime self: @This(), name: []const u8) bool {
        inline for (self.readonly_fields) |field| {
            if (std.mem.eql(u8, field, name)) {
                return true;
            }
        } else return false;
    }
};

/// Some 128-bit fields are better represented as `java.math.BigInteger`,
/// otherwise they are considered IDs and exposed as an array of bytes.
const big_integer = struct {
    const fields = .{
        "credits_posted",
        "credits_pending",
        "debits_posted",
        "debits_pending",
        "amount",
    };

    fn contains(comptime field: []const u8) bool {
        return comptime blk: for (fields) |value| {
            if (std.mem.eql(u8, field, value)) break :blk true;
        } else false;
    }

    fn contains_any(comptime type_info: anytype) bool {
        return comptime blk: for (type_info.fields) |field| {
            if (contains(field.name)) break :blk true;
        } else false;
    }
};

const type_mappings = .{
    .{ tb.AccountFlags, TypeMapping{
        .name = "AccountFlags",
        .private_fields = &.{"padding"},
        .docs_link = "reference/accounts#flags",
    } },
    .{ tb.TransferFlags, TypeMapping{
        .name = "TransferFlags",
        .private_fields = &.{"padding"},
        .docs_link = "reference/transfers#flags",
    } },
    .{ tb.Account, TypeMapping{
        .name = "AccountBatch",
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{ "debits_pending", "credits_pending", "debits_posted", "credits_posted", "timestamp" },
        .docs_link = "reference/accounts/#",
    } },
    .{ tb.Transfer, TypeMapping{
        .name = "TransferBatch",
        .private_fields = &.{"reserved"},
        .readonly_fields = &.{"timestamp"},
        .docs_link = "reference/transfers/#",
    } },
    .{ tb.CreateAccountResult, TypeMapping{
        .name = "CreateAccountResult",
        .docs_link = "reference/operations/create_accounts#",
    } },
    .{ tb.CreateTransferResult, TypeMapping{
        .name = "CreateTransferResult",
        .docs_link = "reference/operations/create_transfers#",
    } },
    .{ tb.CreateAccountsResult, TypeMapping{
        .name = "CreateAccountResultBatch",
        .readonly_fields = &.{ "index", "result" },
    } },
    .{ tb.CreateTransfersResult, TypeMapping{
        .name = "CreateTransferResultBatch",
        .readonly_fields = &.{ "index", "result" },
    } },
    .{ tb_client.tb_status_t, TypeMapping{
        .name = "InitializationStatus",
    } },
    .{ tb_client.tb_packet_status_t, TypeMapping{
        .name = "PacketStatus",
    } },
    .{ tb_client.tb_packet_acquire_status_t, TypeMapping{
        .name = "PacketAcquireStatus",
        .visibility = .internal,
    } },
};

const auto_generated_code_notice =
    \\//////////////////////////////////////////////////////////
    \\// This file was auto-generated by java_bindings.zig
    \\// Do not manually modify.
    \\//////////////////////////////////////////////////////////
    \\
;

fn java_type(
    comptime Type: type,
) []const u8 {
    switch (@typeInfo(Type)) {
        .Enum => return comptime get_mapped_type_name(Type) orelse @compileError(
            "Type " ++ @typeName(Type) ++ " not mapped.",
        ),
        .Struct => |info| switch (info.layout) {
            .Packed => return comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => return comptime get_mapped_type_name(Type) orelse @compileError(
                "Type " ++ @typeName(Type) ++ " not mapped.",
            ),
        },
        .Int => |info| {
            // For better API ergonomy,
            // we expose 16-bit unsigned integers in Java as "int" instead of "short".
            // Even though, the backing fields are always stored as "short".
            std.debug.assert(info.signedness == .unsigned);
            return switch (info.bits) {
                8 => "byte",
                16, 32 => "int",
                64 => "long",
                else => @compileError("invalid int type"),
            };
        },
        else => @compileError("Unhandled type: " ++ @typeName(Type)),
    }
}

fn get_mapped_type_name(comptime Type: type) ?[]const u8 {
    inline for (type_mappings) |type_mapping| {
        if (Type == type_mapping[0]) {
            return type_mapping[1].name;
        }
    } else return null;
}

fn to_case(
    comptime input: []const u8,
    comptime case: enum { camel, pascal, upper },
) []const u8 {
    // TODO(Zig): Cleanup when this is fixed after Zig 0.11.
    // Without comptime blk, the compiler thinks slicing the output on return happens at runtime.
    return comptime blk: {
        var output: [input.len]u8 = undefined;
        if (case == .upper) {
            break :blk std.ascii.upperString(output[0..], input);
        } else {
            var len: usize = 0;
            var iterator = std.mem.tokenize(u8, input, "_");
            while (iterator.next()) |word| {
                _ = std.ascii.lowerString(output[len..], word);
                output[len] = std.ascii.toUpper(output[len]);
                len += word.len;
            }

            output[0] = switch (case) {
                .camel => std.ascii.toLower(output[0]),
                .pascal => std.ascii.toUpper(output[0]),
                .upper => unreachable,
            };

            break :blk output[0..len];
        }
    };
}

fn emit_enum(
    buffer: *std.ArrayList(u8),
    comptime Type: type,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\{[visibility]s}enum {[name]s} {{
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
    });

    const type_info = @typeInfo(Type).Enum;
    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.is_private(field.name)) continue;

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\
                \\    /**
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        }

        try buffer.writer().print(
            \\    {[enum_name]s}(({[int_type]s}) {[value]d}){[separator]c}
            \\
        , .{
            .enum_name = to_case(field.name, .pascal),
            .int_type = int_type,
            .value = @intFromEnum(@field(Type, field.name)),
            .separator = if (i == type_info.fields.len - 1) ';' else ',',
        });
    }

    try buffer.writer().print(
        \\
        \\    public final {[int_type]s} value;
        \\
        \\    {[name]s}({[int_type]s} value) {{
        \\        this.value = value;
        \\    }}
        \\
        \\    public static {[name]s} fromValue({[int_type]s} value) {{
        \\        var values = {[name]s}.values();
        \\        if (value < 0 || value >= values.length)
        \\            throw new IllegalArgumentException(
        \\                    String.format("Invalid {[name]s} value=%d", value));
        \\
        \\        return values[value];
        \\    }}
        \\}}
        \\
        \\
    , .{
        .int_type = int_type,
        .name = mapping.name,
    });
}

fn emit_packed_enum(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime int_type: []const u8,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\{[visibility]s}interface {[name]s} {{
        \\    {[int_type]s} NONE = ({[int_type]s}) 0;
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
        .int_type = int_type,
    });

    inline for (type_info.fields, 0..) |field, i| {
        if (comptime mapping.is_private(field.name)) continue;

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\
                \\    /**
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        }

        try buffer.writer().print(
            \\    {[int_type]s} {[enum_name]s} = ({[int_type]s}) (1 << {[value]d});
            \\
        , .{
            .int_type = int_type,
            .enum_name = to_case(field.name, .upper),
            .value = i,
        });
    }

    try buffer.writer().print("\n", .{});

    inline for (type_info.fields) |field| {
        if (comptime mapping.is_private(field.name)) continue;

        try buffer.writer().print(
            \\    static boolean has{[flag_name]s}(final {[int_type]s} flags) {{
            \\        return (flags & {[enum_name]s}) == {[enum_name]s};
            \\    }}
            \\
            \\
        , .{
            .flag_name = to_case(field.name, .pascal),
            .int_type = int_type,
            .enum_name = to_case(field.name, .upper),
        });
    }

    try buffer.writer().print(
        \\}}
        \\
    , .{});
}

fn batch_type(comptime Type: type) []const u8 {
    switch (@typeInfo(Type)) {
        .Int => |info| {
            std.debug.assert(info.signedness == .unsigned);
            switch (info.bits) {
                16 => return "UInt16",
                32 => return "UInt32",
                64 => return "UInt64",
                else => {},
            }
        },
        .Struct => |info| switch (info.layout) {
            .Packed => return batch_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
            else => {},
        },
        .Enum => return batch_type(std.meta.Int(.unsigned, @bitSizeOf(Type))),
        else => {},
    }

    @compileError("Unhandled type: " ++ @typeName(Type));
}

fn emit_batch(
    buffer: *std.ArrayList(u8),
    comptime type_info: anytype,
    comptime mapping: TypeMapping,
    comptime size: usize,
) !void {
    try buffer.writer().print(
        \\{[notice]s}
        \\package com.tigerbeetle;
        \\
        \\import java.nio.ByteBuffer;
        \\{[big_integer_import]s}
        \\
        \\{[visibility]s}final class {[name]s} extends Batch {{
        \\
        \\    interface Struct {{
        \\        int SIZE = {[size]d};
        \\
        \\
    , .{
        .visibility = if (mapping.visibility == .internal) "" else "public ",
        .notice = auto_generated_code_notice,
        .name = mapping.name,
        .size = size,
        .big_integer_import = if (big_integer.contains_any(type_info))
            "import java.math.BigInteger;"
        else
            "",
    });

    // Fields offset:
    var offset: usize = 0;
    inline for (type_info.fields) |field| {
        try buffer.writer().print(
            \\        int {[field_name]s} = {[offset]d};
            \\
        , .{
            .field_name = to_case(field.name, .pascal),
            .offset = offset,
        });

        offset += @sizeOf(field.type);
    }

    // Constructors:
    try buffer.writer().print(
        \\    }}
        \\
        \\    static final {[name]s} EMPTY = new {[name]s}(0);
        \\
        \\    /**
        \\     * Creates an empty batch with the desired maximum capacity.
        \\     * <p>
        \\     * Once created, an instance cannot be resized, however it may contain any number of elements
        \\     * between zero and its {{@link #getCapacity capacity}}.
        \\     *
        \\     * @param capacity the maximum capacity.
        \\     * @throws IllegalArgumentException if capacity is negative.
        \\     */
        \\    public {[name]s}(final int capacity) {{
        \\        super(capacity, Struct.SIZE);
        \\    }}
        \\
        \\    {[name]s}(final ByteBuffer buffer) {{
        \\        super(buffer, Struct.SIZE);
        \\    }}
        \\
        \\
    , .{
        .name = mapping.name,
    });

    // Properties:
    inline for (type_info.fields) |field| {
        if (field.type == u128) {
            try emit_u128_batch_accessors(buffer, mapping, field);
        } else {
            try emit_batch_accessors(buffer, mapping, field);
        }
    }

    try buffer.writer().print(
        \\}}
        \\
        \\
    , .{});
}

fn emit_batch_accessors(
    buffer: *std.ArrayList(u8),
    comptime mapping: TypeMapping,
    comptime field: anytype,
) !void {
    comptime assert(field.type != u128);
    const is_private = comptime mapping.is_private(field.name);
    const is_read_only = comptime mapping.is_read_only(field.name);

    // Get:
    try buffer.writer().print(
        \\    /**
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    if (comptime trait.is(.Array)(field.type)) {
        try buffer.writer().print(
            \\    {[visibility]s}byte[] get{[property]s}() {{
            \\        return getArray(at(Struct.{[property]s}), {[array_len]d});
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
            .array_len = @typeInfo(field.type).Array.len,
        });
    } else {
        try buffer.writer().print(
            \\    {[visibility]s}{[java_type]s} get{[property]s}() {{
            \\        final var value = get{[batch_type]s}(at(Struct.{[property]s}));
            \\        return {[return_expression]s};
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .java_type = java_type(field.type),
            .property = to_case(field.name, .pascal),
            .batch_type = batch_type(field.type),
            .return_expression = comptime if (trait.is(.Enum)(field.type))
                get_mapped_type_name(field.type).? ++ ".fromValue(value)"
            else
                "value",
        });
    }

    // Set:
    try buffer.writer().print(
        \\    /**
        \\     * @param {[param_name]s}
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{
        .param_name = to_case(field.name, .camel),
    });

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    if (comptime trait.is(.Array)(field.type)) {
        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(byte[] {[param_name]s}) {{
            \\        if ({[param_name]s} == null)
            \\            {[param_name]s} = new byte[{[array_len]d}];
            \\        if ({[param_name]s}.length != {[array_len]d})
            \\            throw new IllegalArgumentException("Reserved must be {[array_len]d} bytes long");
            \\        putArray(at(Struct.{[property]s}), {[param_name]s});
            \\    }}
            \\
            \\
        , .{
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
            .visibility = if (is_private or is_read_only) "" else "public ",
            .array_len = @typeInfo(field.type).Array.len,
        });
    } else {
        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final {[java_type]s} {[param_name]s}) {{
            \\        put{[batch_type]s}(at(Struct.{[property]s}), {[param_name]s}{[value_expression]s});
            \\    }}
            \\
            \\
        , .{
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
            .visibility = if (is_private or is_read_only) "" else "public ",
            .batch_type = batch_type(field.type),
            .java_type = java_type(field.type),
            .value_expression = if (comptime trait.is(.Enum)(field.type))
                ".value"
            else
                "",
        });
    }
}

// We offer multiple APIs for dealing with Uint128 in Java:
// - A byte array, heap-allocated, for ids and user_data;
// - A BigInteger, heap-allocated, for balances and amounts;
// - Two 64-bit integers (long), stack-allocated, for both cases;
fn emit_u128_batch_accessors(
    buffer: *std.ArrayList(u8),
    comptime mapping: TypeMapping,
    comptime field: anytype,
) !void {
    comptime assert(field.type == u128);
    const is_private = comptime mapping.is_private(field.name);
    const is_read_only = comptime mapping.is_read_only(field.name);

    if (big_integer.contains(field.name)) {
        // Get BigInteger:
        try buffer.writer().print(
            \\    /**
            \\     * @return a {{@link java.math.BigInteger}} representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\
        , .{});

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}BigInteger get{[property]s}() {{
            \\        final var index = at(Struct.{[property]s});
            \\        return UInt128.asBigInteger(
            \\            getUInt128(index, UInt128.LeastSignificant), 
            \\            getUInt128(index, UInt128.MostSignificant));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
        });
    } else {
        // Get array:
        try buffer.writer().print(
            \\    /**
            \\     * @return an array of 16 bytes representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\
        , .{});

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}byte[] get{[property]s}() {{
            \\        return getUInt128(at(Struct.{[property]s}));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private) "" else "public ",
            .property = to_case(field.name, .pascal),
        });
    }

    // Get long:
    try buffer.writer().print(
        \\    /**
        \\     * @param part a {{@link UInt128}} enum indicating which part of the 128-bit value is to be retrieved.
        \\     * @return a {{@code long}} representing the first 8 bytes of the 128-bit value if
        \\     *         {{@link UInt128#LeastSignificant}} is informed, or the last 8 bytes if
        \\     *         {{@link UInt128#MostSignificant}}.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}long get{[property]s}(final UInt128 part) {{
        \\        return getUInt128(at(Struct.{[property]s}), part);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private) "" else "public ",
        .property = to_case(field.name, .pascal),
    });

    if (big_integer.contains(field.name)) {
        // Set BigInteger:
        try buffer.writer().print(
            \\    /**
            \\     * @param {[param_name]s} a {{@link java.math.BigInteger}} representing the 128-bit value.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
            \\
        , .{
            .param_name = to_case(field.name, .camel),
        });

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final BigInteger {[param_name]s}) {{
            \\        putUInt128(at(Struct.{[property]s}), UInt128.asBytes({[param_name]s}));
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private or is_read_only) "" else "public ",
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
        });
    } else {
        // Set array:
        try buffer.writer().print(
            \\    /**
            \\     * @param {[param_name]s} an array of 16 bytes representing the 128-bit value.
            \\     * @throws IllegalArgumentException if {{@code {[param_name]s}}} is not 16 bytes long.
            \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
            \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
            \\
        , .{
            .param_name = to_case(field.name, .camel),
        });

        if (mapping.docs_link) |docs_link| {
            try buffer.writer().print(
                \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
                \\     */
                \\
            , .{
                .docs_link = docs_link,
                .field_name = field.name,
            });
        } else {
            try buffer.writer().print(
                \\     */
                \\
            , .{});
        }

        try buffer.writer().print(
            \\    {[visibility]s}void set{[property]s}(final byte[] {[param_name]s}) {{
            \\        putUInt128(at(Struct.{[property]s}), {[param_name]s});
            \\    }}
            \\
            \\
        , .{
            .visibility = if (is_private or is_read_only) "" else "public ",
            .property = to_case(field.name, .pascal),
            .param_name = to_case(field.name, .camel),
        });
    }

    // Set long:
    try buffer.writer().print(
        \\    /**
        \\     * @param leastSignificant a {{@code long}} representing the first 8 bytes of the 128-bit value.
        \\     * @param mostSignificant a {{@code long}} representing the last 8 bytes of the 128-bit value.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}void set{[property]s}(final long leastSignificant, final long mostSignificant) {{
        \\        putUInt128(at(Struct.{[property]s}), leastSignificant, mostSignificant);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private or is_read_only) "" else "public ",
        .property = to_case(field.name, .pascal),
    });

    // Set long without most significant bits
    try buffer.writer().print(
        \\    /**
        \\     * @param leastSignificant a {{@code long}} representing the first 8 bytes of the 128-bit value.
        \\     * @throws IllegalStateException if not at a {{@link #isValidPosition valid position}}.
        \\     * @throws IllegalStateException if a {{@link #isReadOnly() read-only}} batch.
        \\
    , .{});

    if (mapping.docs_link) |docs_link| {
        try buffer.writer().print(
            \\     * @see <a href="https://docs.tigerbeetle.com/{[docs_link]s}{[field_name]s}">{[field_name]s}</a>
            \\     */
            \\
        , .{
            .docs_link = docs_link,
            .field_name = field.name,
        });
    } else {
        try buffer.writer().print(
            \\     */
            \\
        , .{});
    }

    try buffer.writer().print(
        \\    {[visibility]s}void set{[property]s}(final long leastSignificant) {{
        \\        putUInt128(at(Struct.{[property]s}), leastSignificant, 0);
        \\    }}
        \\
        \\
    , .{
        .visibility = if (is_private or is_read_only) "" else "public ",
        .property = to_case(field.name, .pascal),
    });
}

pub fn generate_bindings(
    comptime ZigType: type,
    comptime mapping: TypeMapping,
    buffer: *std.ArrayList(u8),
) !void {
    @setEvalBranchQuota(100_000);

    switch (@typeInfo(ZigType)) {
        .Struct => |info| switch (info.layout) {
            .Auto => @compileError(
                "Only packed or extern structs are supported: " ++ @typeName(ZigType),
            ),
            .Packed => try emit_packed_enum(
                buffer,
                info,
                mapping,
                comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
            ),
            .Extern => try emit_batch(
                buffer,
                info,
                mapping,
                @sizeOf(ZigType),
            ),
        },
        .Enum => try emit_enum(
            buffer,
            ZigType,
            mapping,
            comptime java_type(std.meta.Int(.unsigned, @bitSizeOf(ZigType))),
        ),
        else => @compileError("Type cannot be represented: " ++ @typeName(ZigType)),
    }
}

pub fn main() !void {
    // Emit Java declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
        defer arena.deinit();
        const allocator = arena.allocator();

        var buffer = std.ArrayList(u8).init(allocator);
        try generate_bindings(ZigType, mapping, &buffer);

        try std.fs.cwd().writeFile(
            output_path ++ mapping.name ++ ".java",
            buffer.items,
        );
    }
}

const testing = std.testing;

test "bindings java" {
    // Test Java declarations.
    inline for (type_mappings) |type_mapping| {
        const ZigType = type_mapping[0];
        const mapping = type_mapping[1];

        var buffer = std.ArrayList(u8).init(testing.allocator);
        defer buffer.deinit();

        try generate_bindings(ZigType, mapping, &buffer);

        const current = try std.fs.cwd().readFileAlloc(
            testing.allocator,
            output_path ++ mapping.name ++ ".java",
            std.math.maxInt(usize),
        );
        defer testing.allocator.free(current);

        try testing.expectEqualStrings(buffer.items, current);
    }
}
