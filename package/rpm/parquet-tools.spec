Name:           parquet-tools
Version:        CHANGEME
Release:        1%{?dist}
Summary:        Utility to deal with Parquet data
License:        BSD
Provides:       %{name} = %{version}
Source0:        %{name}-%{version}.tar.gz

%description
Utility to deal with Parquet data, for changelog visit https://github.com/hangxie/parquet-tools/releases

%global debug_package %{nil}

%prep
%autosetup

%build
cp /tmp/%{name}.gz %{name}.gz
gunzip %{name}.gz

%install
install -Dpm 0755 %{name} %{buildroot}%{_bindir}/%{name}

%files
%{_bindir}/%{name}
