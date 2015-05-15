%define scidb_boost scidb-SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR-libboost
%define scidb_path  /opt/scidb/SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR/3rdparty/boost

Name:    %{scidb_boost}
Summary: The free peer-reviewed portable C++ source libraries
Version: 1.54.0
Release: SCIDB_VERSION_PATCH
License: Boost
URL:     http://www.boost.org/
Group:   System Environment/Libraries
Source:  http://sourceforge.net/projects/boost/files/boost/1.54.0/boost_1_54_0.tar.bz2

# 
Patch0: 001-coroutine.patch
# Upstream maintenance fixes from http://www.boost.org/patches.
Patch1: 002-date-time.patch
# Fix conflict between chrono/duration.hpp and apache2/httpd.h.
# The former defines a type named "CR" while the latter #defines CR.
# Closes: #703325.
Patch2: chrono-duration.patch
# Patch example Jamroot to use installed libboost_python.
# Closes: #452410.
Patch3: boost-python-examples.patch
# Ensure Debian systems use endian.h.
# As of 2008-10-27, it seems that __GLIBC__ is no longer defined.
# Assume all Debian systems have <endian.h>.
# Include <endian.h> unconditionally on all Debian systems.
Patch4: endian.patch
# Enable building with Python 3.1
# Fix a build problem where PyInt_Type is not defined.
# Closes: #595786.
Patch5: boost-1.44-py3.1.patch
# Fix jam option --python-buildid.
Patch6: pythonid.patch
# Fix include of <boost/mpi.hpp> on gcc < 4.6.3
# Workaround proposed in boost Trac ticket to allow using -std=c++0x with gcc 4.6.
# It turns out to be a gcc bug that is fixed for gcc 4.6.3 and 4.7.
# Closes: #639862.
Patch7: mpi-allocator-c++0x.patch
# python3.3 has an extra multiarch include location.
# Closes: #691378.
Patch8: fix-ftbfs-python-3.3.patch
# boost1.54 currently FTBFS on hurd-i386 because it doesn't know hurd-i386 has clock_gettime.
# hurd-i386 does not define _POSIX_TIMERS because it  does not have timer_create & co yet,
# but it does have clock_gettime, thus the attached patch.
# Closes: #714847.
Patch9: hurd-clock-gettime.patch
# Suppress compiler warnings under GCC -pedantic. -- Jonathon Bell <jonathon.work@gmail.com>
Patch10: compiler-warnings.patch
#  https://svn.boost.org/trac/boost/ticket/9166
Patch11: boost-container_leak.patch
# Suppress empty macro argument warnings
Patch12: boost-empty_macro.patch
# https://svn.boost.org/trac/boost/ticket/9064
# select_on_container_copy_construction for scoped_allocator segfaults
Patch13: boost-containers-1.55.patch

%define srcdir boost_1_54_0
%define _docdir %{scidb_path}/doc
%define _libdir %{scidb_path}/lib

Requires: %{scidb_boost}-date-time       = %{version}-%{release}
Requires: %{scidb_boost}-filesystem      = %{version}-%{release}
Requires: %{scidb_boost}-graph           = %{version}-%{release}
Requires: %{scidb_boost}-iostreams       = %{version}-%{release}
Requires: %{scidb_boost}-program-options = %{version}-%{release}
Requires: %{scidb_boost}-python          = %{version}-%{release}
Requires: %{scidb_boost}-regex           = %{version}-%{release}
Requires: %{scidb_boost}-serialization   = %{version}-%{release}
Requires: %{scidb_boost}-signals         = %{version}-%{release}
Requires: %{scidb_boost}-system          = %{version}-%{release}
Requires: %{scidb_boost}-test            = %{version}-%{release}
Requires: %{scidb_boost}-thread          = %{version}-%{release}
Requires: %{scidb_boost}-wave            = %{version}-%{release}
Requires: %{scidb_boost}-random          = %{version}-%{release}
Requires: %{scidb_boost}-math            = %{version}-%{release} 
Requires: %{scidb_boost}-atomic          = %{version}-%{release}
Requires: %{scidb_boost}-chrono          = %{version}-%{release}
Requires: %{scidb_boost}-context         = %{version}-%{release}
Requires: %{scidb_boost}-locale          = %{version}-%{release}
Requires: %{scidb_boost}-log             = %{version}-%{release}
Requires: %{scidb_boost}-log_setup       = %{version}-%{release}
Requires: %{scidb_boost}-timer           = %{version}-%{release}

BuildRequires: libstdc++-devel
BuildRequires: bzip2-libs
BuildRequires: bzip2-devel
BuildRequires: zlib-devel
BuildRequires: python-devel
BuildRequires: libicu-devel
BuildRequires: chrpath
BuildRequires: python-libs
#BuildRequires: openmpi-libs

%description
Boost provides free peer-reviewed portable C++ source libraries. The
emphasis is on libraries which work well with the C++ Standard
Library, in the hopes of establishing "existing practice" for
extensions and providing reference implementations so that the Boost
libraries are suitable for eventual standardization. (Some of the
libraries have already been proposed for inclusion in the C++
Standards Committee's upcoming C++ Standard Library Technical Report.)

%package date-time
Summary: Runtime component of the Boost Date-Time library.
Group:   System Environment/Libraries
%description date-time
Runtime support for the Boost Date-Time library, a set of date-time libraries
based on generic programming concepts.

%package filesystem
Summary: Runtime component of the Boost Filesystem library.
Group:   System Environment/Libraries
%description filesystem
Runtime support for the Boost Filesystem library, which provides
portable facilities to query and manipulate paths, files, and
directories.

%package graph
Summary: Runtime component of the Boost Graph library.
Group:   System Environment/Libraries
%description graph
Runtime support for the Boost Graph library. The BGL interface and graph
components are generic, in the same sense as the the Standard Template
Library (STL).

%package iostreams
Summary: Runtime component of the Boost IOStreams library.
Group:   System Environment/Libraries
%description iostreams
Runtime support for the Boost IOStreams library, a framework for defining streams,
stream buffers and i/o filters.

%package math
Summary: A stub that used to contain the Boost Math library.
Group:   System Environment/Libraries
%description math
This package is a stub that used to contain runtime support for the Boost
math library, which is now header-only. It's kept around only so that during 
yum-assisted updates, old libraries from %{scidb_boost}-math package aren't 
left around.

%package program-options
Summary: Runtime component of the Boost Program Options library.
Group:   System Environment/Libraries
%description program-options
Runtime support for the Boost Program Options library, which allows program
developers to obtain (name, value) pairs from the user, via
conventional methods such as command line and configuration file.

%package python
Summary: Runtime component of the Boost Python library.
Group:   System Environment/Libraries
%description python
The Boost Python library is a framework for interfacing Python and
C++. It allows you to quickly and seamlessly expose C++ classes
functions and objects to Python, and vice versa, using no special
tools -- just your C++ compiler.  This package contains runtime
support for the Boost Python library.

%package regex
Summary: Runtime component of the Boost Regular Expression library.
Group:   System Environment/Libraries
%description regex
Runtime support for the Boost Regular Expression library.

%package serialization
Summary: Runtime component of the Boost Serialization library.
Group:   System Environment/Libraries
%description serialization
Runtime support for the Boost Serialization library, which implements 
persistence and marshaling.

%package signals
Summary: Runtime component of the Boost Signals and Slots library.
Group:   System Environment/Libraries
%description signals
Runtime support for the Boost Signals and SLots library.

%package system
Summary: Runtime component of the Boost System Support library.
Group:   System Environment/Libraries
%description system
Runtime support for the Boost Operating System Support library, including
the diagnostics support that will be part of the C++0x standard library.

%package test
Summary: Runtime component of the Boost Test library.
Group:   System Environment/Libraries
%description test
Runtime support for simple program testing, full unit testing, and for
program execution monitoring.

%package thread
Summary: Runtime component of the Boost Thread library.
Group:   System Environment/Libraries
%description thread
Runtime support for the Boost Thread library, which provides classes and
functions for managing multiple threads of execution, and for synchronizing 
data between the threads or providing separate copies of data specific to 
individual threads.

%package wave
Summary: Runtime component of the Boost C99/C++ Preprocessing library.
Group:   System Environment/Libraries
%description wave
Runtime support for the Boost Wave library, a Standards conformant,
and highly configurable implementation of the mandated C99/C++
preprocessor functionality.

%package random
Summary: Runtime component of the Boost Random library.
Group:   System Environment/Libraries
%description random
RUntime support for the Boost Random Number library, which provides a
variety of generators and distributions to produce random numbers having
useful properties, such as uniform distribution.

%package atomic
Summary: Runtime component of the Boost Atomic library.
Group:   System Environment/Libraries
%description atomic
Runtime support for the Boost Atomic library. C++11-style atomic<>.

%package chrono
Summary: Runtime component of the Boost Chrono library.
Group:   System Environment/Libraries
%description chrono
Runtime support for the Boost Chrono library. Useful time utilities.

%package context
Summary: Runtime component of the Boost Context library.
Group:   System Environment/Libraries
%description context
Runtime support for the Boost Context library. Context switching support.

%package locale
Summary: Runtime component of the Boost Locale library.
Group:   System Environment/Libraries
%description locale
Runtime support for the Boost Locale library, which provides localization and
Unicode handling tools for C++.

%package log
Summary: Runtime component of the Boost Log library.
Group:   System Environment/Libraries
%description log
Runtime support for the Boost Log library. Support for logging.

%package log_setup
Summary: Runtime component of the Boost Log-Setup library.
Group:   System Environment/Libraries
%description log_setup
Runtime support for the Boost Log-Setup library.

%package timer
Summary: Runtime component of the Boost Timer library.
Group:   System Environment/Libraries
%description timer
Runtime support for the Boost Timer library, which provides vvent timer, 
progress timer, and progress display classes.

%package devel
Summary: The Boost C++ headers and shared development libraries.
Group:   Development/Libraries
Requires: %{scidb_boost} = %{version}-%{release}
Provides: %{scidb_boost}-python-devel = %{version}-%{release}
%description devel
Headers and shared object symlinks for the Boost C++ libraries.

%package static
Summary: The Boost C++ static development libraries.
Group: Development/Libraries
Requires:  %{scidb_boost}-devel = %{version}-%{release}
Obsoletes: %{scidb_boost}-devel-static < 1.34.1-14
Provides:  %{scidb_boost}-devel-static = %{version}-%{release}
%description static
Static Boost C++ libraries.

%package doc
Summary: HTML documentation for the Boost C++ libraries.
Group: Documentation
BuildArch: noarch
Provides: %{scidb_boost}-python-docs = %{version}-%{release}
%description doc
This package contains the documentation in the HTML format of the Boost C++
libraries. The documentation provides the same content as that on the Boost
web page (http://www.boost.org/doc/libs/1_40_0).

%prep
%setup -q -n %{srcdir}
#Apply any necessary patches here: 
%patch0 -p1
%patch1 -p1
%patch2 -p1
%patch3 -p1
%patch4 -p1
%patch5 -p1
%patch6 -p1
%patch7 -p1
%patch8 -p1
%patch9 -p1
%patch10 -p1
%patch11 -p1
%patch12 -p1
%patch13 -p1

%build
./bootstrap.sh --prefix=/usr
./bjam --without-mpi %{?_smp_mflags}

%install
rm -rf $RPM_BUILD_ROOT
cd %{_builddir}/%{srcdir}/
./bjam --prefix=$RPM_BUILD_ROOT/usr install
mkdir -p "$RPM_BUILD_ROOT/%{scidb_path}"
mv       "$RPM_BUILD_ROOT/usr/lib"     "$RPM_BUILD_ROOT/%{scidb_path}"
mv       "$RPM_BUILD_ROOT/usr/include" "$RPM_BUILD_ROOT/%{scidb_path}"

%clean
rm -rf  $RPM_BUILD_ROOT

%post   date-time       -p /sbin/ldconfig
%postun date-time       -p /sbin/ldconfig
%post   filesystem      -p /sbin/ldconfig
%postun filesystem      -p /sbin/ldconfig
%post   graph           -p /sbin/ldconfig
%postun graph           -p /sbin/ldconfig
%post   iostreams       -p /sbin/ldconfig
%postun iostreams       -p /sbin/ldconfig
%post   program-options -p /sbin/ldconfig
%postun program-options -p /sbin/ldconfig
%post   python          -p /sbin/ldconfig
%postun python          -p /sbin/ldconfig
%post   regex           -p /sbin/ldconfig
%postun regex           -p /sbin/ldconfig
%post   serialization   -p /sbin/ldconfig
%postun serialization   -p /sbin/ldconfig
%post   signals         -p /sbin/ldconfig
%postun signals         -p /sbin/ldconfig
%post   system          -p /sbin/ldconfig
%postun system          -p /sbin/ldconfig
%post   test            -p /sbin/ldconfig
%postun test            -p /sbin/ldconfig
%post   thread          -p /sbin/ldconfig
%postun thread          -p /sbin/ldconfig
%post   wave            -p /sbin/ldconfig
%postun wave            -p /sbin/ldconfig
%post   random          -p /sbin/ldconfig
%postun random          -p /sbin/ldconfig
%post   atomic          -p /sbin/ldconfig
%postun atomic          -p /sbin/ldconfig
%post   chrono          -p /sbin/ldconfig
%postun chrono          -p /sbin/ldconfig
%post   context         -p /sbin/ldconfig
%postun context         -p /sbin/ldconfig
%post   locale          -p /sbin/ldconfig
%postun locale          -p /sbin/ldconfig
%post   log             -p /sbin/ldconfig
%postun log             -p /sbin/ldconfig
%post   log_setup       -p /sbin/ldconfig
%postun log_setup       -p /sbin/ldconfig
%post   timer           -p /sbin/ldconfig
%postun timer           -p /sbin/ldconfig

%files

%files date-time
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_date_time*.so.%{version}

%files filesystem
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_filesystem*.so.%{version}

%files graph
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_graph.so.%{version}

%files iostreams
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_iostreams*.so.%{version}

%files math
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_math*.so.%{version}

%files test
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_prg_exec_monitor*.so.%{version}
%{_libdir}/libboost_unit_test_framework*.so.%{version}

%files program-options
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_program_options*.so.%{version}

%files python
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_python*.so.%{version}

%files regex
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_regex*.so.%{version}

%files serialization
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_serialization*.so.%{version}
%{_libdir}/libboost_wserialization*.so.%{version}

%files signals
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_signals*.so.%{version}

%files system
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_system*.so.%{version}

%files thread
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_thread*.so.%{version}

%files wave
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_wave*.so.%{version}

%files random
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_random*.so.%{version}

%files atomic
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_atomic*.so.%{version}

%files chrono
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_chrono*.so.%{version}

%files context
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_context*.so.%{version}

%files locale
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_locale*.so.%{version}

%files log
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_log*.so.%{version}

%files log_setup
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_log_setup*.so.%{version}

%files timer
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/libboost_timer*.so.%{version}

%files doc
%defattr(-, root, root, -)
%doc %{_docdir}/scidb-SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR-libboost*-%{version}

%files devel
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{scidb_path}/include/boost
%{_libdir}/libboost_*.so

%files static
%defattr(-, root, root, -)
%doc LICENSE_1_0.txt
%{_libdir}/*.a
