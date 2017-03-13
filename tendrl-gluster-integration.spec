%define use_systemd (0%{?fedora} && 0%{?fedora} >= 18) || (0%{?rhel} && 0%{?rhel} >= 7)
Name: tendrl-gluster-integration
Version: 1.2.1
Release: 1%{?dist}
BuildArch: noarch
Summary: Module for Gluster Integration
Source0: %{name}-%{version}.tar.gz
License: LGPLv2+
URL: https://github.com/Tendrl/gluster-integration

BuildRequires: python2-devel
BuildRequires: pytest
BuildRequires: python-mock
BuildRequires: python-dateutil
BuildRequires: python-gevent
BuildRequires: python-greenlet
%if %{use_systemd}
BuildRequires: systemd
%endif

Requires: python-etcd
Requires: python-dateutil
Requires: python-gevent
Requires: python-greenlet
Requires: pytz
Requires: tendrl-commons
Requires: systemd
Requires: gstatus

%description
Python module for Tendrl gluster bridge to manage gluster tasks.

%prep
%setup

# Remove bundled egg-info
rm -rf %{name}.egg-info

%build
%{__python} setup.py build

# remove the sphinx-build leftovers
rm -rf html/.{doctrees,buildinfo}

%install
%{__python} setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
install -m  0755 --directory $RPM_BUILD_ROOT%{_var}/log/tendrl/gluster-integration
install -m  0755  --directory $RPM_BUILD_ROOT%{_sysconfdir}/tendrl/gluster-integration
%if %{use_systemd}
install -Dm 0644 tendrl-gluster-integration.service $RPM_BUILD_ROOT%{_unitdir}/tendrl-gluster-integration.service
%else
install -D -m 755 gluster-integration.el6.service %{buildroot}%{_initrddir}/gluster-integration
%endif
install -Dm 0644 etc/tendrl/gluster-integration/gluster-integration.conf.yaml.sample $RPM_BUILD_ROOT%{_datadir}/tendrl/gluster-integration/gluster-integration.conf.yaml
install -Dm 0644 etc/tendrl/gluster-integration/logging.yaml.timedrotation.sample $RPM_BUILD_ROOT%{_sysconfdir}/tendrl/gluster-integration/gluster-integration_logging.yaml
install -Dm 644 etc/tendrl/gluster-integration/*.sample $RPM_BUILD_ROOT%{_datadir}/tendrl/gluster-integration/

%post
%if %use_systemd
%systemd_post tendrl-gluster-integration.service
%else
/sbin/chkconfig --add gluster-integration
%endif

%preun
%if %use_systemd
%systemd_preun tendrl-gluster-integration.service
%else
/sbin/service gluster-integration stop > /dev/null 2>&1
/sbin/chkconfig --del gluster-integration
%endif

%postun
%systemd_postun_with_restart tendrl-gluster-integration.service

%check
%if %use_systemd
py.test -v tendrl/gluster_integration/tests || :
%endif

%files -f INSTALLED_FILES
%dir %{_var}/log/tendrl/gluster-integration
%dir %{_sysconfdir}/tendrl/gluster-integration
%doc README.rst
%license LICENSE
%config %{_datadir}/tendrl/gluster-integration/gluster-integration.conf.yaml
%if %{use_systemd}
%{_unitdir}/tendrl-gluster-integration.service
%else
%{_initrddir}/gluster-integration
%endif
%config %{_sysconfdir}/tendrl/gluster-integration/gluster-integration_logging.yaml
%{_datadir}/tendrl/gluster-integration


%changelog
* Mon Oct 24 2016 Timothy Asir Jeyasingh <tjeyasin@redhat.com> - 0.0.1-1
- Initial build.
