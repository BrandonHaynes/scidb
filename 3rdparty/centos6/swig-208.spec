Summary: Simplified Wrapper and Interface Generator
Name: swig2
Version: 2.0.8
Release: 1
URL: http://www.swig.org/
Source: http://prdownloads.sourceforge.net/swig/swig-%{version}.tar.gz
License: BSD
Group: Development/Tools

BuildRequires: pcre-devel

%description
SWIG is a software development tool that connects programs written in C and C++
with a variety of high-level programming languages. SWIG is primarily used with
common scripting languages such as Perl, Python, Tcl/Tk, and Ruby, however the
list of supported languages also includes non-scripting languages such as Java,
OCAML and C#. Also several interpreted and compiled Scheme implementations
(Guile, MzScheme, Chicken) are supported. SWIG is most commonly used to create
high-level interpreted or compiled programming environments, user interfaces,
and as a tool for testing and prototyping C/C++ software. SWIG can also export
its parse tree in the form of XML and Lisp s-expressions. 

%prep
%setup -q -n swig-%{version}

%build
%configure
make %{?_smp_mflags}

%install
rm -rf ${RPM_BUILD_ROOT}
make DESTDIR=$RPM_BUILD_ROOT install
#rename swig to swig2 so we can easily coexist with swig-1*
mv ${RPM_BUILD_ROOT}/%{_bindir}/swig ${RPM_BUILD_ROOT}/%{_bindir}/swig2
mv ${RPM_BUILD_ROOT}/%{_bindir}/ccache-swig ${RPM_BUILD_ROOT}/%{_bindir}/ccache-swig2
mv ${RPM_BUILD_ROOT}/usr/share/man/man1/ccache-swig.1 ${RPM_BUILD_ROOT}/usr/share/man/man1/ccache-swig2.1

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
%doc ANNOUNCE CHANGES INSTALL LICENSE LICENSE-GPL LICENSE-UNIVERSITIES README RELEASENOTES
%doc Doc/*
%{_bindir}/*
/usr/share/*
