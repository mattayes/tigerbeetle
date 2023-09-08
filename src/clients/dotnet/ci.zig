const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    // Unit tests.
    try shell.exec("dotnet build --configuration Release", .{});
    // Disable coverage on CI, as it is flaky, see
    // <https://github.com/coverlet-coverage/coverlet/issues/865>
    try shell.exec(
        \\dotnet test
        \\    /p:CollectCoverage=false
        \\    /p:Threshold="95,85,95"
        \\    /p:ThresholdType="line,branch,method"
    , .{});

    // Integration tests.
    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        var sample_dir = try shell.project_root.openDir(
            "src/clients/dotnet/samples/" ++ sample,
            .{},
        );
        defer sample_dir.close();

        try sample_dir.setAsCwd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec("dotnet run", .{});
    }
}

pub fn verify_release(shell: *Shell, gpa: std.mem.Allocator, tmp_dir: std.fs.Dir) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
    defer tmp_beetle.deinit(gpa);

    try shell.exec("dotnet new console", .{});
    try shell.exec("dotnet add package tigerbeetle", .{});

    try Shell.copy_path(
        shell.project_root,
        "src/clients/dotnet/samples/basic/Program.cs",
        tmp_dir,
        "Program.cs",
    );
    try shell.exec("dotnet run", .{});
}
