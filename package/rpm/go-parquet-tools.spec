Name:           go-parquet-tools
Version:        CHANGEME
Release:        1%{?dist}
Summary:        Utility to deal with Parquet data
License:        BSD-3-Clause
Provides:       %{name} = %{version}
Source0:        %{name}-%{version}.tar.gz

%undefine source_date_epoch_from_changelog

%description
Utility to deal with Parquet data, for changelog visit https://github.com/hangxie/parquet-tools/releases

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
