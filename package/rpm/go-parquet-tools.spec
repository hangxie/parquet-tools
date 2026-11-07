Name:           go-parquet-tools
Version:        CHANGEME
Release:        1%{?dist}
Summary:        Command-line tools for Parquet files
License:        BSD-3-Clause
URL:            https://github.com/hangxie/parquet-tools
Source0:        %{name}-%{version}.tar.gz

%undefine source_date_epoch_from_changelog

%description
Inspect, query, and transform Parquet files on local and remote storage.

%global debug_package %{nil}

%prep
%autosetup

%build
cp /tmp/parquet-tools.gz parquet-tools.gz
gunzip parquet-tools.gz

%install
install -Dpm 0755 parquet-tools %{buildroot}%{_bindir}/parquet-tools

%files
%{_bindir}/parquet-tools
