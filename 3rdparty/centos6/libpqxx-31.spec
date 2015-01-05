Name:           libpqxx
Version:        3.1
Release:        1
Summary:        C++ client API for PostgreSQL

Group:          System Environment/Libraries
License:        BSD
URL:            http://pqxx.org/
Source:         http://pqxx.org/download/software/libpqxx/libpqxx-3.1.tar.gz

BuildRequires:  postgresql-devel
BuildRequires:  pkgconfig

%description
C++ client API for PostgreSQL. The standard front-end (in the sense of
"language binding") for writing C++ programs that use PostgreSQL.
Supersedes older libpq++ interface.

%package devel
Summary:        Development tools for libpqxx
Group:          Development/Libraries
Requires:       %{name} = %{version}-%{release}
Requires:       pkgconfig
Requires:       postgresql-devel

%description devel
The libpgxx-devel package contains the header files and static
libraries necessary for developing programs which use libpqxx.

%prep
%setup -q

%build
%configure --enable-shared --disable-static
# I hate rpath... ;)
%{__perl} -pi -e 's/hardcode_into_libs=yes/hardcode_into_libs=no/;' \
libtool
make %{?_smp_mflags} -j%{?jobs:1}


%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT %{?_smp_mflags}

rm -f $RPM_BUILD_ROOT%{_libdir}/lib*.la
%{__perl} -pi -e 's,-R/usr/lib,,' $RPM_BUILD_ROOT%{_bindir}/pqxx-config

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig

%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%doc AUTHORS ChangeLog COPYING NEWS README VERSION
%{_libdir}/libpqxx-*.so

%files devel
%defattr(-,root,root,-)
%doc README-UPGRADE
%{_bindir}/pqxx-config
%{_libdir}/libpqxx.so
%{_includedir}/pqxx
%{_libdir}/pkgconfig/libpqxx.pc
