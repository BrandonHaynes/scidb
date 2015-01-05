Name: log4cxx
Version: 0.10.0
Release: 1
Summary: A port to C++ of the Log4j project

Group: System Environment/Libraries
License: ASL 2.0
URL: http://logging.apache.org/log4cxx/index.html
Source0: http://www.apache.org/dist/logging/log4cxx/%{version}/apache-%{name}-%{version}.tar.gz
Patch0: log4cxx-cstring.patch
BuildRoot:	%{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires: apr-devel,apr-util-devel,doxygen

%description
Log4cxx is a popular logging package written in C++. One of its distinctive
features is the notion of inheritance in loggers. Using a logger hierarchy it
is possible to control which log statements are output at arbitrary
granularity. This helps reduce the volume of logged output and minimize the
cost of logging.

%prep
%setup -q -n apache-%{name}-%{version}
%patch0 -p1

%build
sed -i.libdir_syssearch -e \
 '/sys_lib_dlsearch_path_spec/s|/usr/lib |/usr/lib /usr/lib64 /lib /lib64 |' \
 configure
%configure
make -k %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
make install DESTDIR=$RPM_BUILD_ROOT INSTALL="install -p"
mv $RPM_BUILD_ROOT%{_datadir}/%{name}/html .

%clean
rm -rf $RPM_BUILD_ROOT

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%files
%defattr(-,root,root,-)
%{_libdir}/liblog4cxx.so.10.0.0
%{_libdir}/liblog4cxx.so.10

%doc NOTICE LICENSE KEYS

%package devel
Requires: %{name} = %{version}-%{release},pkgconfig,apr-devel
Group: Development/Libraries
Summary: Header files for Log4xcc - a port to C++ of the Log4j project

%description devel
Header files and documentation you can use to develop with log4cxx

%files devel
%defattr(-,root,root,-)
%exclude %{_libdir}/*.la
%exclude %{_libdir}/*.a
%{_includedir}/log4cxx
%{_libdir}/liblog4cxx.so
%{_libdir}/pkgconfig/liblog4cxx.pc
%doc html/

