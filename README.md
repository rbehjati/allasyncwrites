# AsyncWrite and a Tale of Four Implementations

This repository is home to four alternative implementations of `AsyncWrite`. The following table summarizes them:

| Variant | Path                                         | Is struct stateful? | Calls an async function? | Custom Future? | Pinning of the nested Future |
| ------- | -------------------------------------------- | ------------------- | ------------------------ | -------------- | ---------------------------- |
| Impl-1  | [src/bin/pinned.rs](src/bin/pinned.rs)       | No                  | Yes                      | No             | Pinned field                 |
| Impl-2  | [src/bin/stateful.rs](src/bin/stateful.rs)   | Yes                 | Yes                      | No             | Pinned field                 |
| Impl-3  | [src/bin/pinless.rs](src/bin/pinless.rs)     | Yes                 | No                       | Yes (`Unpin`)  | Safe on-the-fly pinning      |
| Impl-4  | [src/bin/asyncless.rs](src/bin/asyncless.rs) | No (but could be)   | No                       | Yes (`!Unpin`) | `pin_project` or unsafe      |




