Summary: Python command-line parsing library
Name: python-argparse
Version: 1.2.1
Release: 1
URL: http://pypi.python.org/pypi/argparse
Source: http://argparse.googlecode.com/files/argparse-%{version}.tar.gz
License: Python
Group: Development/Tools
BuildArch: noarch
Requires: python

BuildRequires: python
BuildRequires: python-setuptools

%description
The argparse module makes it easy to write user friendly command line interfaces.
The program defines what arguments it requires, and argparse will figure out how
to parse those out of sys.argv. The argparse module also automatically generates
help and usage messages and issues errors when users give the program invalid arguments.
As of Python >= 2.7 and >= 3.2, the argparse module is maintained within the Python
standard library. For users who still need to support Python < 2.7 or < 3.2, it is
also provided as a separate package, which tries to stay compatible with the module
in the standard library, but also supports older Python versions.

%prep
%setup -q -n argparse-%{version}

%build
%{__python} setup.py build

%install
rm -rf ${RPM_BUILD_ROOT}
%{__python} setup.py install --skip-build --root $RPM_BUILD_ROOT

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
%doc LICENSE.txt PKG-INFO NEWS.txt README.txt
%doc doc/*
%{python_sitelib}/*
