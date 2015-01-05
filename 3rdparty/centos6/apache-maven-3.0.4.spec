Name:           apache-maven
Version:        3.0.4
Release:        1
Summary:        Apache Maven
Group:          Development/Tools
License:        Apache
BuildArch:      noarch
URL:            http://maven.apache.org/
Source:         http://apache-mirror.rbc.ru/pub/apache/maven/maven-3/3.0.4/binaries/apache-maven-3.0.4-bin.tar.gz

%description
Apache Maven is a software project management and comprehension tool. 
ased on the concept of a project object model (POM), Maven can manage
a project's build, reporting and documentation from a central piece
of information.

%prep
%setup -q

%build

%install
mkdir -p $RPM_BUILD_ROOT%{_bindir}
mkdir -p $RPM_BUILD_ROOT/usr/share/maven/bin
mkdir -p $RPM_BUILD_ROOT/usr/share/maven/boot
mkdir -p $RPM_BUILD_ROOT/usr/share/maven/lib

install -p -D bin/mvn $RPM_BUILD_ROOT/usr/share/maven/bin/
install -p -D bin/mvnDebug $RPM_BUILD_ROOT/usr/share/maven/bin/
install -m644 -p -D bin/m2.conf $RPM_BUILD_ROOT/usr/share/maven/bin/
install -m644 -p -D boot/*.jar $RPM_BUILD_ROOT/usr/share/maven/boot/
install -m644 -p -D lib/*.jar $RPM_BUILD_ROOT/usr/share/maven/lib/
ln -sf /usr/share/maven/bin/mvn $RPM_BUILD_ROOT%{_bindir}/mvn
ln -sf /usr/share/maven/bin/mvnDebug $RPM_BUILD_ROOT%{_bindir}/mvnDebug
%clean

%files
%defattr(-,root,root,-)
/usr/*