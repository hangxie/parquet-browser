Name:           parquet-browser
Version:        CHANGEME
Release:        1%{?dist}
Summary:        Browse parquet file content in an interactive way
License:        BSD-3-Clause
Provides:       %{name} = %{version}
Source0:        %{name}-%{version}.tar.gz

%description
Browse parquet file content in an interactive way, for changelog visit https://github.com/hangxie/parquet-browser/releases

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
