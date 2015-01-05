# Set to bcond_without or use --with bootstrap if bootstrapping a new release
# or architecture
%bcond_with bootstrap
# Set to bcond_with or use --without gui to disable qt4 gui build
%bcond_without gui
# Set to RC version if building RC, else %{nil}
#%define rcver %{nil}

Name:           cmake
Version:        2.8.10
Release:        1
Summary:        Cross-platform make system

Group:          Development/Tools

# Most sources are BSD. Source/CursesDialog/form/ a bunch is MIT.
# Source/kwsys/MD5.c is bundled(md5-deutsch) and zlib licensed. Some
# GPL-licensed bison-generated files, these all include an exception
# granting redistribution under terms of your choice
License:        BSD and MIT and zlib
URL:            http://www.cmake.org
Source:        http://www.cmake.org/files/v2.8/cmake-%{version}.tar.gz
#Source2:        macros.cmake28
# Patch to find DCMTK in Fedora (bug #720140)
#Patch0:         cmake-dcmtk.patch
# (modified) Upstream patch to fix setting PKG_CONFIG_FOUND (bug #812188)
#Patch1:         cmake-pkgconfig.patch
# This patch renames the executables with a "28" suffix
#Patch2:         cmake28.patch

# Source/kwsys/MD5.c
# see https://fedoraproject.org/wiki/Packaging:No_Bundled_Libraries
#Provides: bundled(md5-deutsch)

#BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  gcc-gfortran
BuildRequires:  ncurses-devel, libX11-devel
BuildRequires:  bzip2-devel
BuildRequires:  curl-devel
BuildRequires:  expat-devel
BuildRequires:  libarchive-devel
BuildRequires:  zlib-devel

%if %{without bootstrap}
#BuildRequires: xmlrpc-c-devel
%endif
#%if %{with gui}
#BuildRequires: qt4-devel, desktop-file-utils
#%define qt_gui --qt-gui
#%endif
Requires:       rpm


%description
CMake is used to control the software compilation process using simple 
platform and compiler independent configuration files. CMake generates 
native makefiles and workspaces that can be used in the compiler 
environment of your choice. CMake is quite sophisticated: it is possible 
to support complex environments requiring system configuration, preprocessor
generation, code generation, and template instantiation.


#%package        gui
#Summary:        Qt GUI for %{name}
#Group:          Development/Tools
#Requires:       %{name}%{?_isa} = %{version}-%{release}
#
#%description    gui
#The %{name}-gui package contains the Qt based GUI for CMake.


%prep
%setup -q -n cmake-%{version}
#%patch0 -p1 -b .dcmtk
#%patch1 -p1 -b .pkgconfig
#%patch2 -p1 -b .cmake28


%build
export CFLAGS="$RPM_OPT_FLAGS"
export CXXFLAGS="$RPM_OPT_FLAGS"
mkdir build
cd build
../bootstrap --prefix=%{_prefix} --datadir=/share/%{name} \
             --docdir=/share/doc/%{name}-%{version} --mandir=/share/man \
             --%{?with_bootstrap:no-}system-libs \
             --parallel=`/usr/bin/getconf _NPROCESSORS_ONLN` \
             --no-qt-gui
make VERBOSE=1 %{?_smp_mflags}


%install
rm -rf $RPM_BUILD_ROOT
cd build
make install DESTDIR=$RPM_BUILD_ROOT
find $RPM_BUILD_ROOT/%{_datadir}/%{name}/Modules -type f | xargs chmod -x
cd ..
#cp -a Example $RPM_BUILD_ROOT%{_docdir}/%{name}-%{version}/
#mkdir -p $RPM_BUILD_ROOT%{_datadir}/emacs/site-lisp/cmake28
#install -m 0644 Docs/cmake-mode.el $RPM_BUILD_ROOT%{_datadir}/emacs/site-lisp/cmake28/cmake28-mode.el
# RPM macros
#install -p -m0644 -D %{SOURCE2} $RPM_BUILD_ROOT%{_sysconfdir}/rpm/macros.cmake28
#sed -i -e "s|@@CMAKE_VERSION@@|%{version}|" $RPM_BUILD_ROOT%{_sysconfdir}/rpm/macros.cmake28
#touch -r %{SOURCE2} $RPM_BUILD_ROOT%{_sysconfdir}/rpm/macros.cmake28
mkdir -p $RPM_BUILD_ROOT%{_libdir}/%{name}

#%if %{with gui}
## Desktop file
#desktop-file-install --delete-original \
#  --dir=%{buildroot}%{_datadir}/applications \
#  %{buildroot}/%{_datadir}/applications/CMake28.desktop
#%endif


#%check
#unset DISPLAY
#pushd build
##ModuleNotices fails for some unknown reason, and we don't care
##CMake.HTML currently requires internet access
##CTestTestUpload requires internet access
## Currently broken - disable for now
#bin/ctest28 -V -E ModuleNotices -E CMake.HTML -E CTestTestUpload %{?_smp_mflags}
#popd


%clean
rm -rf $RPM_BUILD_ROOT


#%if %{with gui}
#%post gui
#update-desktop-database &> /dev/null || :
#update-mime-database %{_datadir}/mime &> /dev/null || :

#%postun gui
#update-desktop-database &> /dev/null || :
#update-mime-database %{_datadir}/mime &> /dev/null || :
#%endif


%files
%defattr(-,root,root,-)
#%config(noreplace) %{_sysconfdir}/rpm/macros.cmake
%{_docdir}/%{name}-%{version}/
#%if %{with gui}
#%exclude %{_docdir}/%{name}-%{version}/cmake28-gui.*
#%endif
%{_bindir}/ccmake
%{_bindir}/cmake
%{_bindir}/cpack
%{_bindir}/ctest
%{_datadir}/aclocal/cmake.m4
%{_datadir}/%{name}/
%{_mandir}/man1/*
#%exclude %{_mandir}/man1/cmake-gui.1.gz
#%{_datadir}/emacs/site-lisp/cmake
%{_libdir}/%{name}/

#%if %{with gui}
#%files gui
#%defattr(-,root,root,-)
#%{_docdir}/%{name}-%{version}/cmake28-gui.*
#%{_bindir}/cmake28-gui
#%{_datadir}/applications/CMake28.desktop
#%{_datadir}/mime/packages/cmake28cache.xml
#%{_datadir}/pixmaps/CMake28Setup32.png
#%{_mandir}/man1/cmake28-gui.1.gz
#%endif
