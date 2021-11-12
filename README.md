# Fact-specific Proxies

In [FAQ 4](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/faqs#4), Treasury outlines that grantees can use fact-based proxies to qualify applicant income as meeting the statutory requirements for being at 80% of AMI. To help grantees simplify their application process, USDS will has worked with GSA's Office of Evaluation Science to create datasets for fact based proxies and example implementations to demostrate the use of these data.

For an overview of the methodology, see the U.S. Department of the Treasury's [Guidelines for fact-specific proxies](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/service-design/fact-specific-proxies).

In this repo, you will find prototype-level implementations for

* A data pipeline to generate fact-specific proxy data, in the `arp_pipeline` directory. See its README for details
* An API for serving fact-specific proxy data, in the `api` directory
* A git submodule of proxy data, in the `erap-data` directory. These files are big, so only close submodules if you want that.

This data is still at the PROTOTYPE/DRAFT phase currently, so condiser yourself warned.
