%global selinuxtype targeted
%global moduletype  services
%global modulenames tendrl

# todo relable should be enhanced later to specific things
%global relabel_files() %{_sbindir}/restorecon -Rv /
%global _format() export %1=""; for x in %{modulenames}; do %1+=%2; %1+=" "; done;

Name: tendrl-gluster-integration
Version: 1.5.1
Release: 1%{?dist}
BuildArch: noarch
Summary: Module for Gluster Integration
Source0: %{name}-%{version}.tar.gz
License: LGPLv2+
URL: https://github.com/Tendrl/gluster-integration

BuildRequires: systemd
BuildRequires: python2-devel
BuildRequires: pytest
BuildRequires: python-mock

Requires: glusterfs-events
Requires: tendrl-commons
Requires: systemd
Requires: python-blivet
Requires: python-flask

%description
Python module for Tendrl gluster bridge to manage gluster tasks.

%package -n tendrl-node-selinux
License: GPLv2
Group: System Environment/Base
Summary: SELinux Policies for Tendrl Node
BuildArch: noarch
Requires(post): selinux-policy-base, selinux-policy-targeted, policycoreutils, policycoreutils-python libselinux-utils
BuildRequires: selinux-policy selinux-policy-devel

%description -n tendrl-node-selinux
SELinux Policies for Tendrl Node

%prep
%setup

# Remove bundled egg-info
rm -rf %{name}.egg-info

%build
make bzip-selinux-policy
%{__python} setup.py build

# remove the sphinx-build leftovers
rm -rf html/.{doctrees,buildinfo}

%install
%{__python} setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
install -m  0755 --directory $RPM_BUILD_ROOT%{_var}/log/tendrl/gluster-integration
install -m  0755  --directory $RPM_BUILD_ROOT%{_sysconfdir}/tendrl/gluster-integration
install -Dm 0644 tendrl-gluster-integration.service $RPM_BUILD_ROOT%{_unitdir}/tendrl-gluster-integration.service
install -Dm 0644 etc/tendrl/gluster-integration/gluster-integration.conf.yaml.sample $RPM_BUILD_ROOT%{_datadir}/tendrl/gluster-integration/gluster-integration.conf.yaml
install -Dm 0644 etc/tendrl/gluster-integration/logging.yaml.timedrotation.sample $RPM_BUILD_ROOT%{_sysconfdir}/tendrl/gluster-integration/gluster-integration_logging.yaml
install -Dm 644 etc/tendrl/gluster-integration/*.sample $RPM_BUILD_ROOT%{_datadir}/tendrl/gluster-integration/

# Install SELinux interfaces and policy modules
install -d %{buildroot}%{_datadir}/selinux/packages

install -m 0644 selinux/tendrl.pp.bz2 \
	%{buildroot}%{_datadir}/selinux/packages

%post -n tendrl-node-selinux
%_format MODULE %{_datadir}/selinux/packages/tendrl.pp.bz2
%{_sbindir}/semodule -n -s %{selinuxtype} -i $MODULE
if %{_sbindir}/selinuxenabled ; then
    %{_sbindir}/load_policy
    %relabel_files
fi

%post
systemctl enable tendrl-gluster-integration
%systemd_post tendrl-gluster-integration.service

%preun
%systemd_preun tendrl-gluster-integration.service

%postun -n tendrl-node-selinux
if [ $1 -eq 0 ]; then
    %{_sbindir}/semodule -n -r %{modulenames} &> /dev/null || :
    if %{_sbindir}/selinuxenabled ; then
	%{_sbindir}/load_policy
	%relabel_files
    fi
fi

%postun
%systemd_postun_with_restart tendrl-gluster-integration.service

%check
py.test -v tendrl/gluster_integration/tests || :

%files -n tendrl-node-selinux
%defattr(-,root,root,0755)
%attr(0644,root,root) %{_datadir}/selinux/packages/tendrl.pp.bz2

%files -f INSTALLED_FILES
%dir %{_var}/log/tendrl/gluster-integration
%dir %{_sysconfdir}/tendrl/gluster-integration
%doc README.rst
%license LICENSE
%config %{_datadir}/tendrl/gluster-integration/gluster-integration.conf.yaml
%{_unitdir}/tendrl-gluster-integration.service
%config %{_sysconfdir}/tendrl/gluster-integration/gluster-integration_logging.yaml
%{_datadir}/tendrl/gluster-integration


%changelog
* Fri Aug 25 2017 Rohan Kanade <rkanade@redhat.com> - 1.5.1-1
- Release tendrl-gluster-integration v1.5.1

* Tue Aug 08 2017 Rohan Kanade <rkanade@redhat.com> - 1.5.0-1
- Release tendrl-gluster-integration v1.5.0

* Mon Jun 19 2017 Rohan Kanade <rkanade@redhat.com> - 1.4.2-1
- Release tendrl-gluster-integration v1.4.2

* Thu Jun 08 2017 Rohan Kanade <rkanade@redhat.com> - 1.4.1-1
- Release tendrl-gluster-integration v1.4.1

* Fri Jun 02 2017 Rohan Kanade <rkanade@redhat.com> - 1.4.0-1
- Release tendrl-gluster-integration v1.4.0

* Thu May 18 2017 Rohan Kanade <rkanade@redhat.com> - 1.3.0-1
- Release tendrl-gluster-integration v1.3.0

* Tue Apr 18 2017 Rohan Kanade <rkanade@redhat.com> - 1.2.3-1
- Release tendrl-gluster-integration v1.2.3

* Tue Apr 11 2017 Rohan Kanade <rkanade@redhat.com> - 1.2.2-1
- Release tendrl-gluster-integration v1.2.2

* Mon Oct 24 2016 Timothy Asir Jeyasingh <tjeyasin@redhat.com> - 0.0.1-1
- Initial build.
