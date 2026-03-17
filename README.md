This project attempts to pull athlete training data from intervals.icu and present in an executive summary of graphs and tabulated data.
Scripts and architecture were developed using claude.ai

Intervals.icu is a leading post-processor of athletics data (with which I have no affiliation)

The owner wanted to achieve two things.
(1) to perform some additional proprietary athletic metric analysis, for which methods could not easily be identified in Intervals.icu, even using its Javascripting facilities.
(2) to anyway, present an executive summary, in the public domain.

In particular, the metrics of interest surround running and cycling cardiovascular and mechanical "efficiency". Answers to the questions: How fast can one run, how powerfully can one cycle, in the backed off regime?
Can this backed off data be extrapolated to predict key metrics (e.g. cycling FTP and running threshold pace) for those two disciplines, without performing the all-out tests (that Cardiologists generally recommend against - value statement).

This project pulls, daily, .fit files and activities from Intervals.icu. Extracts key data, augments and modifies that data stored in .json containers, and performs some simple modeling. The output is reported on three pages.
