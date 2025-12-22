# Contributing to the GDI Mini-Node

At this stage, this software is in its early stages and functions as a working
proof-of-concept. It needs more testing and more deployments to uncover hidden
issues and unsupported corner-cases. However, the current state is sufficient to
start with the evaluation of this software.

## Reporting Issues And Feature-requests

To establish a clear process for reporting problems, please use the GitHub
issues if you encounter problems :

1. First, [check if the issue is already reported](../../issues).
   - If yes, don't add another duplicate issue.
   - You may comment on the existing issue if you have more details to add.
   - You may add 👀 reaction to let the issue handler know who is affected by
     the problem, especially when feedback is later needed.  
2. [Report the issue](../../issues/new) and fill in the fields.
   - Please provide as much technical details as possible to help the issue
     handler understand the situation.
   - Please use one or more labels to categorise this issue.
   - If you plan to handle the issue yourself, or know someone who is willing to
     do it, assign an owner to the issue.

Issues are the place for outlining solutions, discussions and debates. They are
also the history of developments and changes to the software. Therefore, when
writing to issues, please keep in mind that the discussion should be clear
enough to understand the situation and requirements many years later when the
solution might receive new changes.


## Making Changes

Contributions should follow the following criteria:

1. there is a clear issue reported about the development;
2. the development is contained in an independent branch;
3. the changes are locally tested;
4. [CHANGELOG.md](./CHANGELOG.md) is updated with information about the task;
5. a pull request is submitted once the development is done;
6. the issue is assigned to a reporter for verifying whether the task can be
   closed (if not, the developer can repeat steps 2-4).


## About Governance

The software belong to the GDI community who also has privilege to contribute to
the software. At this stage, the software is not an official product and its
support is defined by the (early) adopters. Therefore, for code reviews, please
ask a fellow developer to review the changes, when the community is still being
formalised.

In the future, if there are a more strict processes required for maintaining
this software, permissions may be revised and restricted to the authorized team.


## About Releases

This project supports releasing Docker images via GitHub Actions into GitHub
Packages.

From a branch:
1. Run the **Docker Image** pipeline from the **Actions** tab for a specific
   commit.
2. The artifact will appear under the repo as `mini-node:<branch>-<short-sha1>`.

From a git tag:
1. Create an issue for the release with a specific version number.
2. Create a git branch for the release.
3. Update the software version in [pyproject.toml](./pyproject.toml).
4. Update [CHANGELOG.md](./CHANGELOG.md) to reflect the new release.
5. Verify the functionality.
6. Create a pull-request and get an approval.
7. Once the pull-request is merged, create a git tag with the version number
   (with `v`-prefix, e.g. `v1.0.0`).
8. The **Docker Image** pipeline is triggered automatically upon the push of the
   git tag.
9. The artifact will appear under the repo as `mini-node:<version>`.


## About the Licence And Intellectual Ownership

The source code of this software belongs to the public domain to enable wider
adoption of this software. The software was developed for the [European Genomic
Data Infrastructure](https://gdi.onemilliongenomes.eu) (GDI) project (2022-2026)
as a potential solution for the  national nodes for exposing the properties of
local genomic datasets. The GDI  project received funding from the European
Union’s Digital Europe Programme under grant agreement number 101081813.

In order to keep the project open, every contributor and every contribution is
expected to comply with the licence.
