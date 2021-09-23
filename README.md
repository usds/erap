# Fact-based Proxies

In [FAQ 4](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-state-local-and-tribal-governments/emergency-rental-assistance-program/faqs#4), Treasury outlines that grantees can use fact-based proxies to qualify applicant income as meeting the statutory requirements for being at 80% of AMI. To help grantees simplify their application process, USDS will work with GSA to create datasets for fact based proxies and example implementations to demostrate the use of these data.

## TL;DR
This is a proposed set of iterations to help grantees setup a fact-based proxy. We'll want to
- validate whether any of the outputs below are useful
- determine whether some other outputs might be more useful
- work to answer the various questions raised by this plan (see Outstanding questions for some examples)
The above will change on the basis of feedback from grantees and other stakeholders.

## Tentative Work Plan
- Phase 1: Start with preparing the data
    - For one state, build a data pipeline and generate data files to look up addresses and determine if they are eligible based on their census data. This will use the underlying work from GSA and be for internal QA.
        - Generate CSV and Parquet files of all addresses in the state, and whether the address falls within the 80% of AMI and 50% of AMI flags, along with the values for those for the address (which will be the same for all addresses in a given census block)
    - Publish a methodology for how these data are created and get it blessed by OGC. This is basically documenting whatever GSA does.
    - For all states with addresses in DOT's address data, generate CSV and Parquet files for the data above.
        - MAYBE: Generate SQLite files for same, since in theory anyone could just ship the sqlite file with their application and use it.
- Phase 2: Write a demo API
    - Write a small, well-documented app (language TBD) that shows how you'd use a single state's data to power an API for querying whether a given address qualifies based on fact based proxy. This would document the threat model for the API and the mitigations we'd use for it, show doing some sort of address normalization on the backend, show what indexes we'd create in the database, and how we'd return the data.
        - I don't expect anyone would actually be able to just wholesale incorporate our API, but I do hope we could show through its tests and documentation what sorts of things grantees would need in their own.
    - MAYBE: Hook up the prototype screens to the API to show how it would work end-to-end. This would require more thinking though for how actually show this.
- Phase 3: Write an API for general use
    - Lots TBD overall here, but the idea would be to actually deploy the API so that grantees could just register and use it without having to create their own implementation. It would work for all states for which we have address data, and we'd need to figure out authentication/api keys, hosting, rate limits, etc.

## Outstanding questions
- Which state should we use as an example?
- Are there any PII concerns with publishing files of all these addresses and their income data? It's based entirely on public data, but also I know combining data sets can change their sensitivity.
- What do we do about states that DOT doesn't have addresses for? We could probably sorta fake it via the address ranges in TIGER data, although I'm not sure yet how.
- When does the census publish updated income data? I think it's in November, in which case we'll need to create new versions of the data then (including having GSA re-generate theirs)
- Should we try to talk to some of the bigger providers, like Neighborly or adhoc (sp?) to see what would be useful to them, since they'll be doing a lot of this work?
