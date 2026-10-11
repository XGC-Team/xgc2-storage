# Benchmarks

Micro-benchmarks for what Core pays per write class and per read. They answer two questions:
what does `Durable` cost against `Relaxed` on the disk that holds the database, and what does a
read cost while the single writer is busy. They are not a load test of Core.

## Run them

```sh
STORAGE_BENCH_DIR=/path/on/the/target/disk \
  go test -p 1 -run '^$' -bench . -benchtime 1000x -count 3 ./engine ./modules/coredata
```

`STORAGE_BENCH_DIR` selects the filesystem (default: the temporary directory). Every benchmark
creates its own database below it and removes it. A durable commit costs one `fsync` of that
filesystem, so the numbers only mean something for the disk they ran on: repeat them on the
machine that will hold Core's database before choosing a class for a new write. `-benchtime
1000x` keeps runs comparable; the benchmarks that read first seed a history of 5,000 Runs, and
the batched one seeds 50,000, with durable commits. A full pass takes about six minutes on
the workspace disk of the table below, nearly all of it seeding.

What is timed:

- `engine`: `Store.Write` of one small row, alone and 100 rows per transaction, Durable and
  Relaxed; 16 concurrent callers; a document compare-and-set (`Batch`, canonical JSON, one
  index) in both classes; a point read with one reader, with four, and with four while a
  background goroutine commits durably.
- `modules/coredata`: `CreateRun` (Durable), `UpdateRunStatus` (Relaxed, one update per
  transaction and fifty), `FinishRun` (Durable), `GetRun` and `ListRuns` (a page of 50 of 5,000
  Runs of one target, with and without payloads). A Run carries about 2.3 KB of frozen inputs.

`syncs/op` is the number of durability barriers the owner asked for per operation (`Stats().Fsyncs`):
one per durable commit, none per relaxed commit, plus two per checkpoint that moved frames. A
durable commit therefore shows 1.0 to 1.05.

## Conditions of the numbers below

Measured on 2026-10-10 at commit `68a7db6` (the later commits change documentation, a comment
in `go.mod` and one test name only). Machine: an 8 vCPU Intel Xeon virtual machine with 16 GB
of memory, Linux 6.12, Go 1.26.5, SQLite 3.51.3 through `modernc.org/sqlite` 1.46.2. The machine
was shared with other build and test jobs, with a load average between 7 and 14 while the three
passes ran, so the 99th percentiles are pessimistic and differences below about 20% are noise.

Three filesystems, because the cost of a durable commit is the cost of an `fsync` there. The
median `fsync` of a 4 KiB append, measured right after the runs with a separate probe:

| Name in the tables | Directory | Median `fsync` | What it stands for |
| --- | --- | ---: | --- |
| workspace disk | `/home/node/workspace/build/storage-bench` (overlayfs) | 0.49 ms (99th percentile 6 ms) | the closest to a real disk here; the numbers to read |
| /tmp | `/tmp` (fuse-overlayfs) | 15 us | cheap `fsync`, but writes cost more than on the other two |
| tmpfs | `/dev/shm` | 0 | no durability cost at all: the CPU cost of the code alone |

## Results

Each cell is the median of three runs with the range (min-max) in parentheses.

### Commits per second (engine, one small row per commit unless noted)

| Benchmark | workspace disk | /tmp | tmpfs |
| --- | ---: | ---: | ---: |
| Durable commit | 1,450 (1,320-1,570) | 12,200 (11,000-12,600) | 50,600 (48,500-51,900) |
| Relaxed commit | 44,600 (44,300-51,000) | 15,100 (14,700-16,500) | 43,100 (32,300-45,600) |
| Durable commit of 100 rows (rows/s) | 95,500 (86,100-99,100) | 205,000 (186,000-217,000) | 262,000 (254,000-280,000) |
| Relaxed commit of 100 rows (rows/s) | 273,000 (260,000-276,000) | 228,000 (228,000-237,000) | 263,000 (254,000-298,000) |
| Durable, 16 concurrent callers | 1,280 (1,160-1,360) | 12,300 (11,300-12,500) | 49,800 (46,500-59,200) |
| Relaxed, 16 concurrent callers | 41,200 (26,600-44,200) | 15,100 (13,200-15,800) | 47,600 (42,800-54,700) |
| Document batch, Durable | 1,170 (1,130-1,430) | 4,870 (4,820-4,890) | 9,170 (7,330-9,840) |
| Document batch, Relaxed | 10,300 (9,070-10,400) | 5,560 (5,260-5,640) | 10,900 (10,500-12,500) |

### Run lifecycle (Core data module, Runs of about 2.3 KB)

| Benchmark | workspace disk | /tmp | tmpfs |
| --- | ---: | ---: | ---: |
| CreateRun (Durable), runs/s | 843 (716-951) | 1,510 (1,470-1,560) | 6,500 (5,620-7,000) |
| UpdateRunStatus (Relaxed), updates/s | 8,310 (8,300-8,720) | 4,950 (4,510-4,960) | 12,100 (11,700-12,200) |
| UpdateRunStatus, 50 per transaction, updates/s | 10,300 (9,120-10,600) | 6,900 (3,340-7,580) | 14,300 (13,800-14,700) |
| FinishRun (Durable), runs/s | 792 (764-851) | 1,070 (728-1,410) | 6,320 (6,240-6,470) |

### Read latency, median, microseconds

| Benchmark | workspace disk | /tmp | tmpfs |
| --- | ---: | ---: | ---: |
| Point read, 1 reader | 11 (9.7-11) | 11 (10-12) | 11 (9.1-11) |
| Point read, 4 readers | 12 (12-16) | 19 (17-19) | 15 (12-16) |
| Point read, 4 readers while durable commits run | 14 (13-15) | 19 (18-19) | 17 (13-22) |
| GetRun (5,000 Runs stored) | 40 (39-45) | 42 (39-42) | 38 (37-38) |
| ListRuns, page of 50, without payloads | 345 (333-352) | 334 (328-354) | 334 (316-348) |
| ListRuns, page of 50, with payloads | 447 (430-452) | 527 (512-583) | 513 (495-517) |

### Read latency, 99th percentile, microseconds

| Benchmark | workspace disk | /tmp | tmpfs |
| --- | ---: | ---: | ---: |
| Point read, 1 reader | 39 (34-42) | 50 (41-53) | 36 (36-54) |
| Point read, 4 readers | 180 (106-180) | 271 (234-284) | 361 (295-567) |
| Point read, 4 readers while durable commits run | 334 (293-391) | 283 (247-290) | 451 (386-738) |
| GetRun (5,000 Runs stored) | 172 (132-259) | 164 (140-4,340) | 153 (134-154) |
| ListRuns, page of 50, without payloads | 736 (708-740) | 740 (736-1,840) | 718 (684-749) |
| ListRuns, page of 50, with payloads | 937 (899-1,000) | 2,860 (2,650-4,890) | 1,240 (1,140-1,850) |

### Durability barriers per operation (workspace disk)

| Operation | Barriers per operation |
| --- | ---: |
| Durable commit | 1.000 |
| Durable commit of 100 rows | 1.006 |
| Document batch, Durable | 1.004 |
| CreateRun | 1.046 |
| FinishRun | 1.018 |
| Relaxed commit | 0.000 |
| Relaxed commit of 100 rows | 0.008 |
| Document batch, Relaxed | 0.004 |
| UpdateRunStatus, one per transaction | 0.008 |
| UpdateRunStatus, 50 per transaction | 0.186 |

## Reading the numbers

**The write class is the `fsync`.** On the workspace disk a Durable commit costs one durability
barrier (about 0.5 ms here) and runs at 1,450 commits per second; a Relaxed commit costs none
and runs at 44,600, thirty-one times as many. On tmpfs, where the barrier is free, the two
classes cost the same and Relaxed shows no advantage. Choose a class by what a power failure may take, never by speed
on a development machine; measure on the disk that will hold the database.

**Rows per transaction amortize the barrier.** A durable commit of 100 rows costs little more
than one of a single row (1.0 ms against 0.7 ms): 95,500 rows per second against 1,450. Related writes belong in one
transaction (`UpdateRunStatus` takes a slice for this). Relaxed commits gain less from batching,
because their cost is already the SQL work.

**Concurrency does not raise durable throughput.** Sixteen concurrent durable callers reach 1,280
commits per second, about what a single caller reaches: there is one writer and SQLite has no group
commit. The writer queue decides who waits, not how much gets done. When one fsync is too slow
for a stream of facts, the answer is a Relaxed class or a batch.

**Reads do not wait for the writer.** A point read takes 11 us at the median with one reader and
14 us while durable commits run in the background. The 99th percentile grows from 39 us to
about 330 us when four readers and a writer compete; that is CPU contention on a shared machine,
not lock waiting, because WAL readers never wait for the writer.

**A Run costs two durable commits.** `CreateRun` runs at 843 per second and `FinishRun` at 792
on this disk: about 2.5 ms of writer time per Run, so one writer sustains roughly 400 Runs per
second if it did nothing else. Status transitions are Relaxed and cost a small part of that:
8,300 per second alone, 10,300 per second when 50 share a transaction.

**Reads of Runs.** `GetRun` takes 40 us. A page of 50 Runs (each with about 2.3 KB of frozen
inputs) takes 0.35 ms without payloads and 0.45 ms with them, which is what `OmitPayloads`
saves. A listing by target, root, session, workflow, creation time or open status is answered from
an index in page order, so these figures do not grow with the length of the history
(`TestRecordQueriesUseTheirIndexes`); the benchmark itself uses 5,000 Runs.

**Barriers per operation.** A durable commit asks for exactly one barrier. `CreateRun` shows
1.046 because a checkpoint, which costs two barriers when it moves frames, runs about every 45
commits while Runs of this size stream in; relaxed commits show a few thousandths for the same
reason. The checkpoint is also what makes relaxed commits durable: at most about a second after
the first commit that follows the previous checkpoint.

## What is not measured

- Power loss. The machine cannot be cut off, so the durable class rests on SQLite's
  `synchronous=FULL` contract and on the barrier count above. `tests/faults` kills a child process
  in the middle of a stream of commits; both classes survive that.
- Histories much larger than 5,000 Runs, and databases larger than a few hundred megabytes.
- More than one process: the database has one owner.
- Disks other than the three above. Repeat the run on the target machine.
