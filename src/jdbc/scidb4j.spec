Name: scidb4j
Version: 0.1
Release: 1
URL: http://scidb.org
Group: Development/Libraries/Java
Summary: SciDB Java and JDBC driver
License: GPLv3
BuildArch: noarch
BuildRequires: ant ant-jdepend java-1.6.0-openjdk-devel junit
Requires: protobuf-java
%description
SciDB Java and JDBC driver

%clean
rm -rf dist build depcache

%build
ant configure
ant

%install
install -d -m 755 %{buildroot}%{_javadir}
install -pm 644 dist/scidb4j.jar %{buildroot}%{_javadir}/%{name}.jar

%files
%defattr(-,root,root,-)
/usr/share/java/scidb4j.jar