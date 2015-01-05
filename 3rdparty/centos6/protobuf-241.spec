Summary:        Protocol Buffers - Google's data interchange format
Name:           protobuf
Version:        2.4.1
Release:        2
License:        BSD
Group:          Development/Libraries
Source:         http://protobuf.googlecode.com/files/%{name}-%{version}.tar.bz2
URL:            http://code.google.com/p/protobuf/
BuildRequires:  libtool pkgconfig

%description
Protocol Buffers are a way of encoding structured data in an efficient
yet extensible format. Google uses Protocol Buffers for almost all of
its internal RPC protocols and file formats.

Protocol buffers are a flexible, efficient, automated mechanism for
serializing structured data – think XML, but smaller, faster, and
simpler. You define how you want your data to be structured once, then
you can use special generated source code to easily write and read
your structured data to and from a variety of data streams and using a
variety of languages. You can even update your data structure without
breaking deployed programs that are compiled against the "old" format.

%package compiler
Summary: Protocol Buffers compiler
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description compiler
This package contains Protocol Buffers compiler for all programming
languages

%package devel
Summary: Protocol Buffers C++ headers and libraries
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}
Requires: %{name}-compiler = %{version}-%{release}
Requires: pkgconfig

%description devel
This package contains Protocol Buffers compiler for all languages and
C++ headers and libraries

%package static
Summary: Static development files for %{name}
Group: Development/Libraries
Requires: %{name} = %{version}-%{release}

%description static
Static libraries for Protocol Buffers

%package lite
Summary: Protocol Buffers LITE_RUNTIME libraries
Group: Development/Libraries

%description lite
Protocol Buffers built with optimize_for = LITE_RUNTIME.

The "optimize_for = LITE_RUNTIME" option causes the compiler to generate code
which only depends libprotobuf-lite, which is much smaller than libprotobuf but
lacks descriptors, reflection, and some other features.

%package lite-devel
Summary: Protocol Buffers LITE_RUNTIME development libraries
Group: Development/Libraries
Requires: %{name}-devel = %{version}-%{release}
Requires: %{name}-lite = %{version}-%{release}

%description lite-devel
This package contains development libraries built with 
optimize_for = LITE_RUNTIME.

The "optimize_for = LITE_RUNTIME" option causes the compiler to generate code
which only depends libprotobuf-lite, which is much smaller than libprotobuf but
lacks descriptors, reflection, and some other features.

%package lite-static
Summary: Static development files for %{name}-lite
Group: Development/Libraries
Requires: %{name}-devel = %{version}-%{release}

%description lite-static
This package contains static development libraries built with 
optimize_for = LITE_RUNTIME.

The "optimize_for = LITE_RUNTIME" option causes the compiler to generate code
which only depends libprotobuf-lite, which is much smaller than libprotobuf but
lacks descriptors, reflection, and some other features.

%package java
Summary: Java Protocol Buffers runtime library
Group:   Development/Languages
BuildRequires:    java-devel >= 1.6
BuildRequires:    jpackage-utils
BuildRequires:    apache-maven
#BuildRequires:    maven-compiler-plugin
#BuildRequires:    maven-install-plugin
#BuildRequires:    maven-jar-plugin
#BuildRequires:    maven-javadoc-plugin
#uildRequires:    maven-resources-plugin
#BuildRequires:    maven-surefire-plugin
#BuildRequires:    maven-antrun-plugin
#BuildRequires:    maven-doxia
#BuildRequires:    maven-doxia-sitetools
Requires:         java
Requires:         jpackage-utils
Conflicts:        %{name}-compiler > %{version}
Conflicts:        %{name}-compiler < %{version}
BuildArch:        noarch

%description java
This package contains Java Protocol Buffers runtime library.

%prep
%setup -q

%build
%configure
make %{?_smp_mflags}

pushd java
mvn package
popd

%install
rm -rf %{buildroot}
make %{?_smp_mflags} install DESTDIR=%{buildroot} STRIPBINARIES=no INSTALL="%{__install} -p" CPPROG="cp -p"
find %{buildroot} -type f -name "*.la" -exec rm -f {} \;

pushd java
install -d -m 755 %{buildroot}%{_javadir}
install -pm 644 target/%{name}-java-%{version}.jar %{buildroot}%{_javadir}/%{name}.jar
popd

%post -p /sbin/ldconfig
%postun -p /sbin/ldconfig

%post lite -p /sbin/ldconfig
%postun lite -p /sbin/ldconfig

%post compiler -p /sbin/ldconfig
%postun compiler -p /sbin/ldconfig

%clean
rm -rf %{buildroot}

%files
%defattr(-, root, root, -)
%{_libdir}/libprotobuf.so.*
%doc CHANGES.txt CONTRIBUTORS.txt COPYING.txt README.txt

%files compiler
%defattr(-, root, root, -)
%{_bindir}/protoc
%{_libdir}/libprotoc.so.*
%doc COPYING.txt README.txt

%files devel
%defattr(-, root, root, -)
%dir %{_includedir}/google
%{_includedir}/google/protobuf/
%{_libdir}/libprotobuf.so
%{_libdir}/libprotoc.so
%{_libdir}/pkgconfig/protobuf.pc
%doc examples/add_person.cc examples/addressbook.proto examples/list_people.cc examples/Makefile examples/README.txt

%files static
%defattr(-, root, root, -)
%{_libdir}/libprotobuf.a
%{_libdir}/libprotoc.a

%files lite
%defattr(-, root, root, -)
%{_libdir}/libprotobuf-lite.so.*

%files lite-devel
%defattr(-, root, root, -)
%{_libdir}/libprotobuf-lite.so
%{_libdir}/pkgconfig/protobuf-lite.pc

%files lite-static
%defattr(-, root, root, -)
%{_libdir}/libprotobuf-lite.a

%files java
%defattr(-, root, root, -)
%{_javadir}/%{name}.jar
