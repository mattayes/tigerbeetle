const std = @import("std");
const builtin = @import("builtin");
const assert = std.debug.assert;
const math = std.math;
const mem = std.mem;

const config = @import("../config.zig");

const TableType = @import("table.zig").TableType;
const TreeType = @import("tree.zig").TreeType;
const GridType = @import("grid.zig").GridType;
const CompositeKey = @import("composite_key.zig").CompositeKey;
const NodePool = @import("node_pool.zig").NodePool(config.lsm_manifest_node_size, 16);

const snapshot_latest = @import("tree.zig").snapshot_latest;

fn ObjectTreeHelpers(comptime Object: type) type {
    assert(@hasField(Object, "id"));
    assert(std.meta.fieldInfo(Object, .id).field_type == u128);
    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).field_type == u64);

    return struct {
        inline fn compare_keys(timestamp_a: u64, timestamp_b: u64) std.math.Order {
            return std.math.order(timestamp_a, timestamp_b);
        }

        inline fn key_from_value(value: *const Object) u64 {
            return value.timestamp;
        }

        const sentinel_key = std.math.maxInt(u64);
        const tombstone_bit = 1 << (64 - 1);

        inline fn tombstone(value: *const Object) bool {
            return (value.timestamp & tombstone_bit) != 0;
        }

        inline fn tombstone_from_key(timestamp: u64) Object {
            var value = std.mem.zeroes(Object); // Full zero-initialized Value.
            value.timestamp = timestamp | tombstone_bit;
            return value;
        }
    };
}

const IdTreeValue = extern struct {
    id: u128,
    timestamp: u64,
    padding: u64 = 0,

    inline fn compare_keys(a: u128, b: u128) std.math.Order {
        return std.math.order(a, b);
    }

    inline fn key_from_value(value: *const IdTreeValue) u128 {
        return value.id;
    }

    // TODO(ifreund): disallow this id in the state machine.
    const sentinel_key = std.math.maxInt(u128);
    const tombstone_bit = 1 << (64 - 1);

    inline fn tombstone(value: *const IdTreeValue) bool {
        return (value.timestamp & tombstone_bit) != 0;
    }

    inline fn tombstone_from_key(id: u128) IdTreeValue {
        return .{
            .id = id,
            .timestamp = tombstone_bit,
        };
    }
};

/// Normalizes index tree field types into either u64 or u128 for CompositeKey
fn IndexCompositeKeyType(comptime Field: type) type {
    switch (@typeInfo(Field)) {
        .Enum => |e| {
            return switch (@bitSizeOf(e.tag_type)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u64)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported enum tag for index: " ++ @typeName(e.tag_type)),
            };
        },
        .Int => |i| {
            if (i.signedness != .unsigned) {
                @compileError("Index int type (" ++ @typeName(Field) ++ ") is not unsigned");
            }
            return switch (@bitSizeOf(Field)) {
                0...@bitSizeOf(u64) => u64,
                @bitSizeOf(u64)...@bitSizeOf(u128) => u128,
                else => @compileError("Unsupported int type for index: " ++ @typeName(Field)),
            };
        },
        else => @compileError("Index type " ++ @typeName(Field) ++ " is not supported"),
    }
}

comptime {
    assert(IndexCompositeKeyType(u1) == u64);
    assert(IndexCompositeKeyType(u16) == u64);
    assert(IndexCompositeKeyType(enum(u16) { x }) == u64);

    assert(IndexCompositeKeyType(u32) == u64);
    assert(IndexCompositeKeyType(u63) == u64);
    assert(IndexCompositeKeyType(u64) == u64);

    assert(IndexCompositeKeyType(enum(u65) { x }) == u128);
    assert(IndexCompositeKeyType(u65) == u128);
    assert(IndexCompositeKeyType(u128) == u128);
}

fn IndexTreeType(
    comptime Storage: type,
    comptime Field: type,
    comptime tree_name: []const u8,
) type {
    const Key = CompositeKey(IndexCompositeKeyType(Field));
    const Table = TableType(
        Key,
        Key.Value,
        Key.compare_keys,
        Key.key_from_value,
        Key.sentinel_key,
        Key.tombstone,
        Key.tombstone_from_key,
    );

    return TreeType(Table, Storage, tree_name);
}

/// A Groove is a collection of LSM trees auto generated for fields on a struct type
/// as well as custom derived fields from said struct type.
pub fn GrooveType(
    comptime Storage: type,
    comptime Object: type,
    /// An anonymous struct instance which contains the following:
    ///
    /// - ignored: [][]const u8:
    ///     An array of fields on the Object type that should not be given index trees
    ///
    /// - derived: { .field = fn (*const Object) ?DerivedType }:
    ///     An anonymous struct which contain fields that don't exist on the Object
    ///     but can be derived from an Object instance using the field's corresponding function.
    comptime options: anytype,
) type {
    assert(@hasField(Object, "id"));
    assert(std.meta.fieldInfo(Object, .id).field_type == u128);
    assert(@hasField(Object, "timestamp"));
    assert(std.meta.fieldInfo(Object, .timestamp).field_type == u64);

    comptime var index_fields: []const std.builtin.TypeInfo.StructField = &.{};

    // Generate index LSM trees from the struct fields.
    inline for (std.meta.fields(Object)) |field| {
        // See if we should ignore this field from the options.
        //
        // By default, we ignore the "timestamp" field since it's a special identifier.
        // Since the "timestamp" is ignored by default, it shouldn't be provided in options.ignored.
        comptime var ignored = mem.eql(u8, field.name, "timestamp") or mem.eql(u8, field.name, "id");
        inline for (options.ignored) |ignored_field_name| {
            comptime assert(!std.mem.eql(u8, ignored_field_name, "timestamp"));
            comptime assert(!std.mem.eql(u8, ignored_field_name, "id"));
            ignored = ignored or std.mem.eql(u8, field.name, ignored_field_name);
        }

        if (!ignored) {
            const tree_name = @typeName(Object) ++ "." ++ field.name;
            const IndexTree = IndexTreeType(Storage, field.field_type, tree_name);
            index_fields = index_fields ++ [_]std.builtin.TypeInfo.StructField{
                .{
                    .name = field.name,
                    .field_type = IndexTree,
                    .default_value = null,
                    .is_comptime = false,
                    .alignment = @alignOf(IndexTree),
                },
            };
        }
    }

    // Generiate IndexTrees for fields derived from the Value in options.
    const derived_fields = std.meta.fields(@TypeOf(options.derived));
    inline for (derived_fields) |field| {
        // Get the function info for the derived field.
        const derive_func = @field(options.derived, field.name);
        const derive_func_info = @typeInfo(@TypeOf(derive_func)).Fn;

        // Make sure it has only one argument.
        if (derive_func_info.args.len != 1) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Make sure the function takes in a reference to the Value:
        const derive_arg = derive_func_info.args[0];
        if (derive_arg.is_generic) @compileError("expected derive fn arg to not be generic");
        if (derive_arg.arg_type != *const Object) {
            @compileError("expected derive fn to take in *const " ++ @typeName(Object));
        }

        // Get the return value from the derived field as the DerivedType.
        const derive_return_type = derive_func_info.return_type orelse {
            @compileError("expected derive fn to return valid tree index type");
        };

        // Create an IndexTree for the DerivedType:
        const tree_name = @typeName(Object) ++ "." ++ field.name;
        const DerivedType = @typeInfo(derive_return_type).Optional.child;
        const IndexTree = IndexTreeType(Storage, DerivedType, tree_name);

        index_fields = index_fields ++ &.{
            .{
                .name = field.name,
                .field_type = IndexTree,
                .default_value = null,
                .is_comptime = false,
                .alignment = @alignOf(IndexTree),
            },
        };
    }

    const ObjectTree = blk: {
        const Table = TableType(
            u64, // key = timestamp
            Object,
            ObjectTreeHelpers(Object).compare_keys,
            ObjectTreeHelpers(Object).key_from_value,
            ObjectTreeHelpers(Object).sentinel_key,
            ObjectTreeHelpers(Object).tombstone,
            ObjectTreeHelpers(Object).tombstone_from_key,
        );

        const tree_name = @typeName(Object);
        break :blk TreeType(Table, Storage, tree_name);
    };

    const IdTree = blk: {
        const Table = TableType(
            u128,
            IdTreeValue,
            IdTreeValue.compare_keys,
            IdTreeValue.key_from_value,
            IdTreeValue.sentinel_key,
            IdTreeValue.tombstone,
            IdTreeValue.tombstone_from_key,
        );

        const tree_name = @typeName(Object) ++ ".id";
        break :blk TreeType(Table, Storage, tree_name);
    };

    const IndexTrees = @Type(.{
        .Struct = .{
            .layout = .Auto,
            .fields = index_fields,
            .decls = &.{},
            .is_tuple = false,
        },
    });

    // Verify no hash collisions between all the trees:
    comptime var hashes: []const u128 = &.{ObjectTree.hash};

    inline for (std.meta.fields(IndexTrees)) |field| {
        const IndexTree = @TypeOf(@field(@as(IndexTrees, undefined), field.name));
        const hash: []const u128 = &.{IndexTree.hash};

        assert(std.mem.containsAtLeast(u128, hashes, 0, hash));
        hashes = hashes ++ hash;
    }

    // Verify groove index count:
    const indexes_count_actual = std.meta.fields(IndexTrees).len;
    const indexes_count_expect = std.meta.fields(Object).len -
        options.ignored.len -
        // The id/timestamp field is implicitly ignored since it's the primary key for ObjectTree:
        2 +
        std.meta.fields(@TypeOf(options.derived)).len;

    assert(indexes_count_actual == indexes_count_expect);

    // Generate a helper function for interacting with an Index field type.
    const IndexTreeFieldHelperType = struct {
        /// Returns true if the field is a derived field.
        fn is_derived(comptime field_name: []const u8) bool {
            comptime var derived = false;
            inline for (derived_fields) |derived_field| {
                derived = derived or std.mem.eql(u8, derived_field.name, field_name);
            }
            return derived;
        }

        /// Gets the index type from the index name (even if the index is derived).
        fn IndexType(comptime field_name: []const u8) type {
            if (!is_derived(field_name)) {
                return @TypeOf(@field(@as(Object, undefined), field_name));
            }

            const derived_fn = @TypeOf(@field(options.derived, field_name));
            return @typeInfo(derived_fn).Fn.return_type.?.Optional.child;
        }

        fn HelperType(comptime field_name: []const u8) type {
            return struct {
                const Index = IndexType(field_name);

                /// Try to extract an index from the object, deriving it when necessary.
                pub fn derive_index(object: *const Object) ?Index {
                    if (comptime is_derived(field_name)) {
                        return @field(options.derived, field_name)(object);
                    } else {
                        return @field(object, field_name);
                    }
                }

                /// Create a Value from the index that can be used in the IndexTree.
                pub fn derive_value(
                    object: *const Object,
                    index: Index,
                ) CompositeKey(IndexCompositeKeyType(Index)).Value {
                    return .{
                        .timestamp = object.timestamp,
                        .field = switch (@typeInfo(Index)) {
                            .Int => index,
                            .Enum => @enumToInt(index),
                            else => @compileError("Unsupported index type for " ++ field_name),
                        },
                    };
                }
            };
        }
    }.HelperType;

    return struct {
        const Groove = @This();

        const Grid = GridType(Storage);

        const Callback = fn (*Groove) void;
        const JoinOp = enum {
            compacting,
            checkpoint,
            open,
        };

        const PrefetchIDs = std.AutoHashMapUnmanaged(u128, void);

        const PrefetchObjectsContext = struct {
            pub fn hash(_: PrefetchObjectsContext, object: Object) u64 {
                return std.hash.Wyhash.hash(0, mem.asBytes(&object.id));
            }

            pub fn eql(_: PrefetchObjectsContext, a: Object, b: Object) bool {
                return a.id == b.id;
            }
        };
        const PrefetchObjectsAdapter = struct {
            pub fn hash(_: PrefetchObjectsAdapter, id: u128) u64 {
                return std.hash.Wyhash.hash(0, mem.asBytes(&id));
            }

            pub fn eql(_: PrefetchObjectsAdapter, a_id: u128, b_object: Object) bool {
                return a_id == b_object.id;
            }
        };
        const PrefetchObjects = std.HashMapUnmanaged(Object, void, PrefetchObjectsContext, 70);

        join_op: ?JoinOp = null,
        join_pending: usize = 0,
        join_callback: ?Callback = null,

        objects_cache: *ObjectTree.ValueCache,
        objects: ObjectTree,

        ids_cache: *IdTree.ValueCache,
        ids: IdTree,

        indexes: IndexTrees,

        /// Object IDs enqueued to be prefetched.
        /// Prefetching ensures that point lookups against the latest snapshot are synchronous.
        /// This shields state machine implementations from the challenges of concurrency and I/O,
        /// and enables simple state machine function signatures that commit writes atomically.
        prefetch_ids: PrefetchIDs,

        /// An auxillary hash map for prefetched objects which are not present in the mutable table
        /// or value cache of the object tree and ID index tree.
        /// This is required for correctness, to not evict other prefetch hits from the value cache.
        prefetch_objects: PrefetchObjects,

        pub fn init(
            allocator: mem.Allocator,
            node_pool: *NodePool,
            grid: *Grid,
            // The cache size is meant to be computed based on the left over available memory
            // that tigerbeetle was given to allocate from CLI arguments.
            cache_size: u32,
            // In general, the commit count max for a field, depends on the field's object,
            // how many objects might be changed by a batch:
            //   (config.message_size_max - sizeOf(vsr.header))
            // For example, there are at most 8191 transfers in a batch.
            // So commit_count_max=8191 for transfer objects and indexes.
            //
            // However, if a transfer is ever mutated, then this will double commit_count_max
            // since the old index might need to be removed, and the new index inserted.
            //
            // A way to see this is by looking at the state machine. If a transfer is inserted,
            // how many accounts and transfer put/removes will be generated?
            //
            // This also means looking at the state machine operation that will generate the
            // most put/removes in the worst case.
            // For example, create_accounts will put at most 8191 accounts.
            // However, create_transfers will put 2 accounts (8191 * 2) for every transfer, and
            // some of these accounts may exist, requiring a remove/put to update the index.
            commit_count_max: u32,
        ) !Groove {
            // Cache is dynamically allocated to pass a pointer into the Object tree.
            const objects_cache = try allocator.create(ObjectTree.ValueCache);
            errdefer allocator.destroy(objects_cache);

            objects_cache.* = .{};
            try objects_cache.ensureTotalCapacity(allocator, cache_size);
            errdefer objects_cache.deinit(allocator);

            // Intialize the object LSM tree.
            var object_tree = try ObjectTree.init(
                allocator,
                node_pool,
                grid,
                objects_cache,
                .{
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer object_tree.deinit(allocator);

            // Cache is dynamically allocated to pass a pointer into the ID tree.
            const ids_cache = try allocator.create(IdTree.ValueCache);
            errdefer allocator.destroy(ids_cache);

            ids_cache.* = .{};
            try ids_cache.ensureTotalCapacity(allocator, cache_size);
            errdefer ids_cache.deinit(allocator);

            var id_tree = try IdTree.init(
                allocator,
                node_pool,
                grid,
                ids_cache,
                .{
                    .commit_count_max = commit_count_max,
                },
            );
            errdefer id_tree.deinit(allocator);

            var index_trees_initialized: usize = 0;
            var index_trees: IndexTrees = undefined;

            // Make sure to deinit initialized index LSM trees on error.
            errdefer inline for (std.meta.fields(IndexTrees)) |field, field_index| {
                if (index_trees_initialized >= field_index + 1) {
                    @field(index_trees, field.name).deinit(allocator);
                }
            };

            // Initialize index LSM trees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(index_trees, field.name) = try field.field_type.init(
                    allocator,
                    node_pool,
                    grid,
                    null, // No value cache for index trees.
                    .{
                        .commit_count_max = commit_count_max,
                    },
                );
                index_trees_initialized += 1;
            }

            // TODO: document why this is twice the commit count max.
            const prefetch_count_max = commit_count_max * 2;

            var prefetch_ids = PrefetchIDs{};
            try prefetch_ids.ensureTotalCapacity(allocator, prefetch_count_max);
            errdefer prefetch_ids.deinit(allocator);

            var prefetch_objects = PrefetchObjects{};
            try prefetch_objects.ensureTotalCapacity(allocator, prefetch_count_max);
            errdefer prefetch_objects.deinit(allocator);

            return Groove{
                .objects_cache = objects_cache,
                .objects = object_tree,

                .ids_cache = ids_cache,
                .ids = id_tree,

                .indexes = index_trees,

                .prefetch_ids = prefetch_ids,
                .prefetch_objects = prefetch_objects,
            };
        }

        pub fn deinit(groove: *Groove, allocator: mem.Allocator) void {
            assert(groove.join_op == null);
            assert(groove.join_pending == 0);
            assert(groove.join_callback == null);

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).deinit(allocator);
            }

            groove.objects.deinit(allocator);
            groove.objects_cache.deinit(allocator);
            allocator.destroy(groove.objects_cache);

            groove.ids.deinit(allocator);
            groove.ids_cache.deinit(allocator);
            allocator.destroy(groove.ids_cache);

            groove.prefetch_ids.deinit(allocator);
            groove.prefetch_objects.deinit(allocator);

            groove.* = undefined;
        }

        pub fn get(groove: *Groove, id: u128) ?*const Object {
            if (groove.ids.get_cached(id)) |id_tree_value| {
                if (id_tree_value.tombstone()) {
                    return null;
                } else {
                    const object = groove.objects.get_cached(id_tree_value.timestamp);
                    assert(!ObjectTreeHelpers(Object).tombstone(object));
                    return object;
                }
            } else if (groove.prefetch_objects.getKeyPtrAdapted(id, PrefetchObjectsAdapter{})) |object| {
                return object;
            } else {
                return null;
            }
        }

        /// Must be called directly after the state machine commit is finished and prefetch results
        /// are no longer needed.
        pub fn prefetch_clear(groove: *Groove) void {
            groove.prefetch_objects.clearRetainingCapacity();
            assert(groove.prefetch_objects.count() == 0);
            assert(groove.prefetch_ids.count() == 0);
        }

        /// This must be called by the state machine for every key to be prefetched.
        pub fn prefetch_enqueue(groove: *Groove, id: u128) void {
            if (groove.ids.get_cached(id) != null) return;

            // We tolerate duplicate IDs enqueued by the state machine.
            // For example, if all unique operations require the same two dependencies.
            groove.prefetch_ids.putAssumeCapacity(id, {});
        }

        /// Ensure the objects corresponding to all ids enqueued with prefetch_enqueue() are
        /// in memory, either in the value cache of the object tree or in the prefetch_objects
        /// backup hash map.
        pub fn prefetch(
            groove: *Groove,
            callback: fn (*PrefetchContext) void,
            context: *PrefetchContext,
        ) void {
            context.* = .{
                .groove = groove,
                .callback = callback,
                .id_iterator = groove.prefetch_ids.keyIterator(),
            };
            context.start_workers();
        }

        pub const PrefetchContext = struct {
            groove: *Groove,
            callback: fn (*PrefetchContext) void,

            id_iterator: PrefetchIDs.KeyIterator,

            /// The goal is to fully utilize the disk I/O to ensure the prefetch completes as
            /// quickly as possible, so we run multiple lookups in parallel based on the max
            /// I/O depth of the Grid.
            workers: [Grid.read_iops_max]PrefetchWorker = undefined,
            /// The number of workers that are currently running in parallel.
            workers_busy: u32 = 0,

            fn start_workers(context: *PrefetchContext) void {
                assert(context.workers_busy == 0);

                while (context.workers_busy < context.workers.len) : (context.workers_busy += 1) {
                    const worker = &context.workers[context.workers_busy];
                    worker.* = .{ .contex = context };
                    if (!worker.lookup_start()) break;
                }

                if (context.workers_busy == 0) {
                    assert(context.groove.prefetch_ids.count() == 0);
                    context.finish();
                }
            }

            fn worker_finished(context: *PrefetchContext) void {
                assert(context.groove.prefetch_ids.count() == 0);

                context.workers_busy -= 0;
                if (context.workers_busy == 0) context.finish();
            }

            fn finish(context: *PrefetchContext) void {
                assert(context.groove.prefetch_ids.count() == 0);

                const callback = context.callback;
                context.* = undefined;
                callback(context);
            }
        };

        pub const PrefetchWorker = struct {
            context: *PrefetchContext,
            // TODO(ifreund): use a union for these to save memory, likely an extern union
            // so that we can safetly @ptrCast() until @fieldParentPtr() is implemented
            // for unions. See: https://github.com/ziglang/zig/issues/6611
            lookup_id: IdTree.LookupContext = undefined,
            lookup_object: ObjectTree.LookupContext = undefined,

            /// Returns true if asynchronous I/O has been started.
            /// Returns false if there are no more IDs to prefetch.
            fn lookup_start(worker: *PrefetchWorker) bool {
                const groove = worker.context.groove;

                while (worker.context.id_iterator.next()) |id| {
                    // Objects cached by the LSM tree don't need to be added to the auxillary
                    // prefetch_objects hash map.
                    if (groove.ids.get_cached(id.*)) |id_tree_value| {
                        if (config.verify) {
                            assert(groove.objects.get_cached(id_tree_value.timestamp) != null);
                        }
                        continue;
                    }

                    // If not in the LSM tree's cache, the object must be read from disk and added
                    // to the auxillary prefetch_objects hash map.
                    // TODO: this LSM tree function needlessly checks the LSM tree's cache a
                    // second time. Adding API to the LSM tree to avoid this may be worthwhile.
                    groove.ids.lookup(
                        lookup_id_callback,
                        &worker.lookup_id,
                        snapshot_latest,
                        id.*,
                    );
                    return true;
                }

                assert(groove.prefetch_ids.count() == 0);
                return false;
            }

            fn lookup_id_callback(
                completion: *IdTree.LookupContext,
                result: ?*const IdTreeValue,
            ) void {
                const worker = @fieldParentPtr(PrefetchWorker, "lookup_id", completion);

                if (result) |id_tree_value| {
                    worker.groove.objects.lookup(
                        lookup_object_callback,
                        &worker.lookup_object,
                        snapshot_latest,
                        id_tree_value.timestamp,
                    );
                } else {
                    worker.lookup_finish();
                }
            }

            fn lookup_object_callback(
                completion: *ObjectTree.LookupContext,
                result: ?*const Object,
            ) void {
                const worker = @fieldParentPtr(PrefetchWorker, "lookup_object", completion);

                // The result must be non-null as we keep the ID and Object trees in sync.
                const object = result.?;
                assert(!ObjectTreeHelpers(Object).tombstone(object));

                worker.context.groove.prefetch_objects.putAssumeCapacity(object.*, {});
                worker.lookup_finish();
            }

            fn lookup_finish(worker: *PrefetchWorker) void {
                if (!worker.lookup_start()) {
                    worker.context.worker_finished();
                    worker.* = undefined;
                }
            }
        };

        pub fn put(groove: *Groove, object: *const Object) void {
            if (groove.get(object.id)) |existing_object| {
                groove.update(existing_object, object);
            } else {
                groove.insert(object);
            }
        }

        /// Insert the value into the objects tree and its fields into the index trees.
        fn insert(groove: *Groove, object: *const Object) void {
            groove.objects.put(object);
            groove.ids.put(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive_index(object)) |index| {
                    const index_value = Helper.derive_value(object, index);
                    @field(groove.indexes, field.name).put(&index_value);
                }
            }
        }

        /// Update the object and index tress by diff'ing the old and new values.
        fn update(groove: *Groove, old: *const Object, new: *const Object) void {
            assert(old.id == new.id);
            assert(old.timestamp == new.timestamp);

            // Update the object tree entry if any of the fields (even ignored) are different.
            if (!std.mem.eql(u8, std.mem.asBytes(old), std.mem.asBytes(new))) {
                groove.objects.remove(old);
                groove.objects.put(new);
            }

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);
                const old_index = Helper.derive_index(old);
                const new_index = Helper.derive_index(new);

                // Only update the indexes that change.
                if (!std.meta.eql(old_index, new_index)) {
                    if (old_index) |index| {
                        const old_index_value = Helper.derive_value(old, index);
                        @field(groove.indexes, field.name).remove(&old_index_value);
                    }

                    if (new_index) |index| {
                        const new_index_value = Helper.derive_value(new, index);
                        @field(groove.indexes, field.name).put(&new_index_value);
                    }
                }
            }
        }

        pub fn remove(groove: *Groove, object: *const Object) void {
            // Make sure the given object exists in the object tree.
            const existing_object = groove.get(object.id).?;
            assert(mem.eql(u8, mem.asBytes(existing_object), mem.asBytes(object)));

            groove.objects.remove(object);
            groove.ids.remove(&IdTreeValue{ .id = object.id, .timestamp = object.timestamp });

            inline for (std.meta.fields(IndexTrees)) |field| {
                const Helper = IndexTreeFieldHelperType(field.name);

                if (Helper.derive_index(object)) |index| {
                    const index_value = Helper.derive_value(object, index);
                    @field(groove.indexes, field.name).remove(&index_value);
                }
            }
        }

        /// Maximum number of pending sync callbacks (ObjecTree + IndexTrees).
        const join_pending_max = 1 + std.meta.fields(IndexTrees).len;

        fn JoinType(comptime join_op: JoinOp) type {
            return struct {
                pub fn start(groove: *Groove, join_callback: Callback) void {
                    // Make sure no sync op is currently running.
                    assert(groove.join_op == null);
                    assert(groove.join_pending == 0);
                    assert(groove.join_callback == null);

                    // Start the sync operations
                    groove.join_op = join_op;
                    groove.join_callback = join_callback;
                    groove.join_pending = join_pending_max;
                }

                /// Returns LSM tree type for the given index field name (or ObjectTree if null).
                fn TreeFor(comptime index_field_name: ?[]const u8) type {
                    const index_field = index_field_name orelse return ObjectTree;
                    return @TypeOf(@field(@as(IndexTrees, undefined), index_field));
                }

                pub fn tree_callback(
                    comptime index_field_name: ?[]const u8,
                ) fn (*TreeFor(index_field_name)) void {
                    return struct {
                        fn tree_cb(tree: *TreeFor(index_field_name)) void {
                            // Derive the groove pointer from the tree using the index_field_name.
                            const groove = blk: {
                                const index_field = index_field_name orelse {
                                    break :blk @fieldParentPtr(Groove, "objects", tree);
                                };

                                const indexes = @fieldParentPtr(IndexTrees, index_field, tree);
                                break :blk @fieldParentPtr(Groove, "indexes", indexes);
                            };

                            // Make sure the sync operation is currently running.
                            assert(groove.join_op == join_op);
                            assert(groove.join_callback != null);
                            assert(groove.join_pending <= join_pending_max);

                            // Guard until all pending sync ops complete.
                            groove.join_pending -= 1;
                            if (groove.join_pending > 0) return;

                            const callback = groove.join_callback.?;
                            groove.join_op = null;
                            groove.join_callback = null;
                            callback(groove);
                        }
                    }.tree_cb;
                }
            };
        }

        pub fn open(groove: *Groove, callback: fn (*Groove) void) void {
            const Join = JoinType(.open);
            Join.start(groove, callback);

            groove.objects.open(Join.tree_callback(null));

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).open(Join.tree_callback(field.name));
            }
        }

        pub fn compact_io(groove: *Groove, op: u64, callback: Callback) void {
            // Start a compacting join operation.
            const Join = JoinType(.compacting);
            Join.start(groove, callback);

            // Compact the ObjectTree.
            groove.objects.compact_io(op, Join.tree_callback(null));

            // Compact the IndexTrees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_io(op, Join.tree_callback(field.name));
            }
        }

        pub fn compact_cpu(groove: *Groove) void {
            // Make sure a compacting join operation is running
            assert(groove.join_op == JoinOp.compacting);
            assert(groove.join_pending <= join_pending_max);
            assert(groove.join_callback != null);

            groove.objects.compact_cpu();

            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).compact_cpu();
            }
        }

        pub fn checkpoint(groove: *Groove, op: u64, callback: fn (*Groove) void) void {
            // Start a checkpoint join operation.
            const Join = JoinType(.checkpoint);
            Join.start(groove, callback);

            // Checkpoint the ObjectTree.
            groove.objects.checkpoint(op, Join.tree_callback(null));

            // Checkpoint the IndexTrees.
            inline for (std.meta.fields(IndexTrees)) |field| {
                @field(groove.indexes, field.name).checkpoint(op, Join.tree_callback(field.name));
            }
        }
    };
}

test "Groove" {
    const Transfer = @import("../tigerbeetle.zig").Transfer;
    const Storage = @import("../storage.zig").Storage;

    const Groove = GrooveType(
        Storage,
        Transfer,
        .{
            .ignored = [_][]const u8{ "reserved", "user_data", "flags" },
            .derived = .{},
        },
    );

    _ = Groove.init;
    _ = Groove.deinit;

    _ = Groove.get;
    _ = Groove.put;
    _ = Groove.remove;

    _ = Groove.compact_io;
    _ = Groove.compact_cpu;
    _ = Groove.checkpoint;
}