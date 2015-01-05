%{!?python_sitelib: %define python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib()")}

Summary: Builds packages inside chroots
Name: mock
Version: 1.1.24
Release: 1
License: GPLv2+
Group: Development/Tools
Source: https://fedorahosted.org/mock/raw-attachment/wiki/MockTarballs/%{name}-%{version}.tar.xz
Source1: centos-6-x86_64.cfg
URL: http://fedoraproject.org/wiki/Projects/Mock
BuildArch: noarch
Requires: python >= 2.6, yum >= 2.4, yum-utils >= 1.1.9, tar, pigz, python-ctypes, python-decoratortools, usermode
Requires(pre): shadow-utils
Requires(post): coreutils
BuildRequires: python-devel

%description
Mock takes an SRPM and builds it in a chroot

%prep
%setup -q
%setup1

%build
%configure
make

%install
rm -rf $RPM_BUILD_ROOT
make %{?_smp_mflags} DESTDIR=$RPM_BUILD_ROOT install
mkdir -p $RPM_BUILD_ROOT/var/lib/mock
mkdir -p $RPM_BUILD_ROOT/var/cache/mock
ln -s consolehelper $RPM_BUILD_ROOT/usr/bin/mock

# compatibility symlinks
# (probably be nuked in the future)
pushd $RPM_BUILD_ROOT/etc/mock
ln -s epel-5-i386.cfg   fedora-5-i386-epel.cfg
ln -s epel-5-ppc.cfg    fedora-5-ppc-epel.cfg
ln -s epel-5-x86_64.cfg fedora-5-x86_64-epel.cfg
# more compat, from devel/rawhide rename
ln -s fedora-rawhide-i386.cfg fedora-devel-i386.cfg
ln -s fedora-rawhide-x86_64.cfg fedora-devel-x86_64.cfg
ln -s fedora-rawhide-ppc.cfg fedora-devel-ppc.cfg
ln -s fedora-rawhide-ppc64.cfg fedora-devel-ppc64.cfg
install -m 0644 %{SOURCE1} .
popd
echo "%defattr(0644, root, mock)" > %{name}.cfgs
find $RPM_BUILD_ROOT%{_sysconfdir}/mock -name "*.cfg" \
    | sed -e "s|^$RPM_BUILD_ROOT|%%config(noreplace) |" >> %{name}.cfgs

# just for %%ghosting purposes
ln -s fedora-rawhide-x86_64.cfg $RPM_BUILD_ROOT%{_sysconfdir}/mock/default.cfg

%clean
rm -rf $RPM_BUILD_ROOT

%pre
if [ $1 -eq 1 ]; then
    groupadd -r mock >/dev/null 2>&1 || :
fi

%post
# TODO: use dist and version of install system, not build one
if [ ! -e %{_sysconfdir}/%{name}/default.cfg ] ; then
    # in case of dangling symlink
    rm -f %{_sysconfdir}/%{name}/default.cfg
    arch=$(uname -i)
    for ver in %{?fedora}%{?rhel} rawhide ; do
        cfg=%{?fedora:fedora}%{?rhel:epel}-$ver-$arch.cfg
        if [ -e %{_sysconfdir}/%{name}/$cfg ] ; then
            ln -s -f $cfg %{_sysconfdir}/%{name}/default.cfg
            exit 0
        fi
    done
fi
# fix cache permissions from old installs
chmod 2775 /var/cache/mock
:

%files -f %{name}.cfgs
%defattr(-, root, root)

# executables
%{_bindir}/mock
%{_bindir}/mockchain
%attr(0755, root, root) %{_sbindir}/mock

# python stuff
%{python_sitelib}/*

# config files
%dir  %{_sysconfdir}/%{name}
%ghost %config(noreplace,missingok) %{_sysconfdir}/%{name}/default.cfg
%config(noreplace) %{_sysconfdir}/%{name}/*.ini
%config(noreplace) %{_sysconfdir}/pam.d/%{name}
%config(noreplace) %{_sysconfdir}/security/console.apps/%{name}
%{_sysconfdir}/bash_completion.d

# docs
%{_mandir}/man1/mock.1*
%{_mandir}/man1/mockchain.1*
%doc ChangeLog

# cache & build dirs
%defattr(0775, root, mock, 02775)
%dir /var/cache/mock
%dir /var/lib/mock
 