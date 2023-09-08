const std = @import("std");
const log = std.log;
const assert = std.debug.assert;

const flags = @import("../../flags.zig");
const fatal = flags.fatal;
const Shell = @import("../../shell.zig");
const TmpTigerBeetle = @import("../../testing/tmp_tigerbeetle.zig");

pub fn tests(shell: *Shell, gpa: std.mem.Allocator) !void {
    // Java's maven doesn't support a separate test command, or a way to add dependency on a
    // project (as opposed to a compiled jar file).
    //
    // For this reason, we do all our testing in one go, imperatively building a client jar and
    // installing it into a temporary maven repository.
    try shell.exec("mvn --batch-mode --file pom.xml --quiet formatter:validate", .{});
    try shell.exec("mvn --batch-mode --file pom.xml --quiet package", .{});

    var tmp_dir = std.testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const maven_repo_path = try tmp_dir.dir.realpathAlloc(shell.arena.allocator(), ".");

    try shell.exec(
        \\mvn deploy:deploy-file --quiet
        \\  -Durl=file://{maven_repo_path}
        \\  -Dfile=target/tigerbeetle-java-0.0.1-SNAPSHOT.jar
        \\  -DgroupId=com.tigerbeetle -DartifactId=tigerbeetle-java -Dversion=0.0.0
        \\  -Dpackaging=jar
    , .{
        .maven_repo_path = maven_repo_path,
    });

    inline for (.{ "basic", "two-phase", "two-phase-many" }) |sample| {
        var sample_dir = try shell.project_root.openDir("src/clients/java/samples/" ++ sample, .{});
        defer sample_dir.close();

        try sample_dir.setAsCwd();

        var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
        defer tmp_beetle.deinit(gpa);

        try shell.env.put("TB_ADDRESS", tmp_beetle.port_str.slice());
        try shell.exec(
            \\mvn --batch-mode --file pom.xml --quiet
            \\  package exec:java -Dmaven.repo.local={maven_repo_path}
        , .{
            .maven_repo_path = maven_repo_path,
        });
    }
}

pub fn verify_release(shell: *Shell, gpa: std.mem.Allocator, tmp_dir: std.fs.Dir) !void {
    var tmp_beetle = try TmpTigerBeetle.init(gpa, .{});
    defer tmp_beetle.deinit(gpa);

    // TODO
    _ = shell;
    _ = tmp_dir;
}
