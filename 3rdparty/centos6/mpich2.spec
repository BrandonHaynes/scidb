%define scidb_mpich2 scidb-SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR-mpich2
%define scidb_path   /opt/scidb/SCIDB_VERSION_MAJOR.SCIDB_VERSION_MINOR/3rdparty/mpich2
%define short_name   mpich2

Summary:	A high-performance implementation of MPI
Name:		%{scidb_mpich2}
Version:	1.2.1
Release:	2.3%{?dist}
License:	MIT
Group:		Development/Libraries
URL:		http://www.mcs.anl.gov/research/projects/mpich2
Source0:	http://downloads.paradigm4.com/centos6.3/3rdparty_sources/mpich2-%{version}.tar.gz
BuildRequires:	libXt-devel, libuuid-devel
BuildRequires:	java-devel-openjdk, gcc-gfortran
BuildRequires:	emacs-common, perl, python
Requires:	python

%define _unpackaged_files_terminate_build 0

%description
MPICH2 is a high-performance and widely portable implementation of the
MPI standard. This release has all MPI-2.1 functions and features
required by the standard with the exeption of support for the
"external32" portable I/O format.

The mpich2 binaries in this RPM packages were configured to use the default
process manager 'MPD' using the default device 'ch3'. The ch3 device
was configured with support for the nemesis channel that allows for
shared-memory and TCP/IP sockets based communication.

This build also include support for using '/usr/sbin/alternatives'
and/or the 'module environment' to select which MPI implementation to use
when multiple implementations are installed.

%package devel
Summary:	Development files for mpich2
Group:		Development/Libraries
Provides:	%{name}-devel-static = %{version}-%{release}
Requires:	%{name} = %{version}-%{release}
Requires:	gcc-gfortran 
ExcludeArch: s390 s390x ppc ppc64

%description devel
Contains development headers and libraries for mpich2

%package doc
Summary:	Documentations and examples for mpich2
Group:          Documentation
BuildArch:      noarch

%description doc
Contains documentations, examples and manpages for mpich2

# We only compile with gcc, but other people may want other compilers.
# Set the compiler here.
%{!?opt_cc: %global opt_cc gcc}
%{!?opt_fc: %global opt_fc gfortran}
%{!?opt_f77: %global opt_f77 gfortran}
# Optional CFLAGS to use with the specific compiler...gcc doesn't need any,
# so uncomment and undefine to NOT use
%{!?opt_cc_cflags: %global opt_cc_cflags %{optflags}}
%{!?opt_fc_fflags: %global opt_fc_fflags %{optflags}}
#%{!?opt_fc_fflags: %global opt_fc_fflags %{optflags} -I%{_fmoddir}}
%{!?opt_f77_fflags: %global opt_f77_fflags %{optflags}}

%ifarch s390
%global m_option -m31
%else
%global m_option -m%{__isa_bits}
%endif

%ifarch %{ix86} x86_64
%global selected_channels ch3:nemesis
%else
%global selected_channels ch3:sock
%endif

%ifarch x86_64 ia64 ppc64 s390x sparc64
%global priority 41
%else
%global priority 40
%endif

%ifarch x86_64 s390
%global XFLAGS -fPIC
%endif

%prep
%setup -q -n %{short_name}-%{version}

%global _prefix      %{scidb_path}
%global _datarootdir %{scidb_path}/share
%global _datadir     %{scidb_path}/share
%global _mandir	     %{_datadir}/man
%global _bindir	     %{_prefix}/bin
%global _libdir	     %{_prefix}/lib
%global _includedir  %{_prefix}/include/%{short_name}

%build
%configure	\
	--enable-sharedlibs=gcc					\
	--enable-f90						\
	--prefix=%{_prefix}					\
	--with-device=%{selected_channels}			\
	--sysconfdir=%{_sysconfdir}/%{short_name}		\
	--includedir=%{_includedir}				\
	--bindir=%{_bindir}					\
	--libdir=%{_libdir}					\
	--datarootdir=%{_datarootdir}				\
	--datadir=%{_datadir}					\
	--mandir=%{_mandir}					\
	--docdir=%{_datadir}/doc				\
	--htmldir=%{_datadir}/doc				\
	--with-java=%{_sysconfdir}/alternatives/java_sdk	\
	F90=%{opt_fc}						\
	F77=%{opt_f77}						\
	CFLAGS="%{m_option} -O2 %{?XFLAGS}"			\
	CXXFLAGS="%{m_option} -O2 %{?XFLAGS}"			\
	F90FLAGS="%{m_option} -O2 %{?XFLAGS}"			\
	FFLAGS="%{m_option} -O2 %{?XFLAGS}"			\
	LDFLAGS='-Wl,-z,noexecstack'				\
	MPICH2LIB_CFLAGS="%{?opt_cc_cflags}"			\
	MPICH2LIB_CXXFLAGS="%{optflags}"			\
	MPICH2LIB_F90FLAGS="%{?opt_fc_fflags}"			\
	MPICH2LIB_FFLAGS="%{?opt_f77_fflags}"	

make VERBOSE=1

%install
rm -rf %{buildroot}
make DESTDIR=%{buildroot} install

mkdir -p %{buildroot}%{_mandir}/man1
rm -f %{buildroot}%{_bindir}/{mpiexec,mpirun,mpdrun}

pushd  %{buildroot}%{_bindir}/
ln -s mpiexec.py mpdrun
touch mpiexec mpirun mpich2version
touch %{buildroot}%{_mandir}/man1/mpiexec.1
popd

cp -pr src/mpe2/README src/mpe2/README.mpe2

# The uninstall script here is not needed/necesary for rpm packaging 
rm -rf %{buildroot}%{_sbindir}/mpe*

find %{buildroot} -type f -name "*.la" -exec rm -f {} ';'

%clean
#rm -rf %{buildroot}


%files
%defattr(-,root,root,-)
%{_bindir}/*
%dir %{_libdir}
%dir %{_bindir}
%{_libdir}/*.jar
%{_libdir}/mpe*.o
%{_libdir}/*.so.*
%{_bindir}/mpiexec
%{_bindir}/mpirun
%{_bindir}/mpich2version
%dir %{_mandir}
%doc %{_mandir}/man1/

%files devel
%defattr(-,root,root,-)
%{_bindir}/mpi*
%{_bindir}/*log*
%{_bindir}/jumpshot
%{_includedir}
%{_libdir}/*.a
%{_libdir}/*.so
%{_libdir}/trace_rlog/libTraceInput.so
%{_datadir}/examples*/Makefile

%files doc
%defattr(-,root,root,-)
%dir %{_datadir}
%{_datadir}/doc/
%{_datadir}/examples*
%{_datadir}/logfiles/
%{_mandir}/man3/
%{_mandir}/man4/
%exclude %{_datadir}/examples*/Makefile
