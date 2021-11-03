Name:           parquet-tools
Version:        1.8.1
Release:        1%{?dist}
Summary:        Utility to deal with Parquet data

License:        BSD

Provides:       %{name} = %{version}
Source0:        %{name}-%{version}.tar.gz

%description
Utility to deal with Parquet data

%global debug_package %{nil}

%prep
%autosetup

%build
cp /tmp/%{name}-%{version}-linux-amd64.gz %{name}.gz
gunzip %{name}.gz

%install
install -Dpm 0755 %{name} %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}

%changelog

* Wed Nov 03 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.8.2

 - release v1.8.2
 - build rpm package

* Sun Oct 31 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.8.1

 - do not run release-build for PR
 - add new unit test for schema cmd
 - add nested type to unit test
 - fix bug in go struct output
 - always test release build
 - update USAGE with more recent version

* Fri Oct 29 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.8.0

 - add go struct output to schema command

* Fri Oct 29 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.7.2

 - generate both sha512 and md5 checksum

* Fri Oct 29 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.7.1

 - generate checksum for released artifacts
 - add link to installation steps to README
 - fix apt warning
 - move back to xitongsys parquet-go and parquet-go-source
 - update to latest parquet-go to avoid unnecessary panic
 - setup CCI timeout to 60 minutes

* Mon Sep 06 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.7.0

 - use jsonl as format name in cat command
 - import from JSONL

* Sun Sep 05 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.6.0

 - import from JSON

* Sun Aug 22 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.5.1

 - use release version of CCI image

* Sun Aug 22 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.5.0

 - update Dockerfile for go 1.17, also always test docker build
 - support JSON streaming format in cat
 - update USAGE for Windows ARM64 build
 - update comment to indicate field renaming in MAP/LIST case
 - go 1.17
 - refactor code for golint
 - update repo badges
 - move back to parquet-go upstream
 - use parquet-go-source fork for azblob bug fix

* Mon May 31 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.4.3

 - fix linux/arm7 docker build

* Sun May 30 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.4.2

 - install ca-certificates to docker image

* Sun May 30 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.4.1

 - fix azure blob URI

* Sun May 30 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.4.0

 - add invalid filter test to cat_Run
 - refact common_test to group tests
 - update unit test for more coverage
 - update URI parameter help message
 - add Azure blob support
 - circleci to slack notification
 - replace parquet-go with fork for bug fixes

* Fri May 28 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.3.0

 - add GCS support
 - minor enhancement on parsing URI

* Fri May 28 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.2.1

 - update USAGE.md to refelct recent docker build changes
 - build docker image for arm/v7

* Thu May 27 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.2.0

 - add USAGE.md
 - push docker image to latest tag
 - fix meta command base64 flag
 - add more operator to filter
 - respect data type during filtering
 - add helper functions for cat filter

* Mon May 24 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.1.0

 - build github release only on tag
 - add filter to cat command
 - update .gitignore for vim
 - update platform to build
 - support brew tap installation

* Sun May 23 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.0.3

 - use buildx to build docker image for multiple platforms

* Sat May 22 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.0.2

 - build docker image
 - add homebrew formula
 - build .exe file packaging in zip for windows

* Sat May 22 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.0.1

 - add code to cat_test to show what we care about
 - deal with sorting columns
 - statistics footer is optional

* Thu May 20 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 1.0.0

 - tweak version default output to meet common sense

* Wed May 19 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.2.1

 - generate changelog along with release build

* Wed May 19 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.2.0

 - discard stdout from cat_test
 - omit empty Children in schema raw output
 - provide option to output min/max value in raw or base64 format

* Wed May 19 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.1.0

 - omit null fields in schema output
 - update README
 - clean up CLI flags

* Tue May 18 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.7

 - fix tag build pattern
 - build binaries and push to github for releases
 - add release-build target to Makefile

* Tue May 18 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.6

 - tricks to improve code coverage
 - output help on error
 - add cat command
 - add JSON output to size and version command
 - remove dump command

* Mon May 17 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.5

 - add meta command
 - close reader/writer whenever possible
 - add footer size flag
 - add short names to flags

* Sat May 15 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.4

 - add size command
 - fix data type
 - refactor unit tests

* Sat May 15 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.3

 - add import command (csv only ATM)
 - add function to capture stdout/stderr for unit test
 - add Writer functions to common
 - generate junit report

* Thu May 06 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.2

 - add schema command
 - update rowcount test
 - add CircleCI build badge
 - add format, lint and build to CI
 - upload test coverage report to CircleCI artifacts
 - fix golint installation
 - enable CI

* Sun May 02 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.1

 - Merge pull request #4 from hangxie/row-count
 - implement row count
 - add newParquetFileReader
 - add unit test for version command
 - Merge pull request #3 from hangxie/fixup
 - another fix
 - Merge pull request #2 from hangxie/fixup
 - fix cmd go.mod
 - Merge pull request #1 from hangxie/initial-commit
 - skeleton

* Fri Apr 30 2021 Hang Xie <7977860+hangxie@users.noreply.github.com> 0.0.0

 - Initial commit
