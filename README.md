# cronutil

This library provides a standard implementation for a distributed cron for use in PotatoBeans standard Go services and
applications.

The distributed cron are used to schedule periodic actions across multiple services. This utilizes Redis to provide a
global lock and the schedule to the action.

## Legal and Acknowledgements

This repository was built by:
* Sergio Ryan \[[sergioryan@potatobeans.id](mailto:sergioryan@potatobeans.id)]
* Audie Masola \[[audiemasola@potatobeans.id](mailto:audiemasola@potatobeans.id)]
* Eka Novendra \[[novendraw@potatobeans.id](mailto:novendraw@potatobeans.id)]
* Stefanus Ardi Mulia \[[stefanusardi@potatobeans.id](mailto:stefanusardi@potatobeans.id)]

Copyright &copy; 2019-2023 PotatoBeans Company (PT Padma Digital Indonesia).  
All rights reserved.