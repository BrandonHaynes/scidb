Name:           pigz
Version:        2.2.5
Release:        1
Summary:        Parallel implementation of gzip
Group:          Applications/File
License:        zlib
URL:            http://www.zlib.net/pigz/
Source:         http://www.zlib.net/%{name}/%{name}-%{version}.tar.gz

BuildRequires:  zlib-devel
BuildRequires:  ncompress

%description
pigz, which stands for parallel implementation of gzip,
is a fully functional replacement for gzip that exploits
multiple processors and multiple cores to the hilt when compressing data.

%prep
%setup -q

%build
make %{?_smp_mflags} CFLAGS='%{optflags}'

%install
rm -rf $RPM_BUILD_ROOT
install -p -D pigz $RPM_BUILD_ROOT%{_bindir}/pigz
pushd $RPM_BUILD_ROOT%{_bindir}; ln pigz unpigz; popd
install -p -D pigz.1 -m 0644 $RPM_BUILD_ROOT%{_datadir}/man/man1/pigz.1

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root,-)
%doc pigz.pdf README
%{_bindir}/pigz
%{_bindir}/unpigz
%{_datadir}/man/man1/pigz.*
