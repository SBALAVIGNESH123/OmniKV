# Fuzz regressions

Checked-in files under this directory are inputs that previously exposed a
panic, hang, data-loss condition, parser crash, or restore/parsing bug.

When fuzzing finds a new case:

1. minimize it with `cargo fuzz tmin <target> <artifact>`,
2. copy the minimized file to `fuzz/regressions/<target>/`,
3. add or update a deterministic regression assertion if the crash needs a
   semantic check beyond "does not panic",
4. run the fuzz smoke tests before opening a PR.
