# ExCELL TravelTimeDailyCurvesService



## Setup

This service computes the hydrograph of traveltime of the graphhopper-edges in the relation sid - next_sid. The data based on the data from the edge_records table, which contains the fcd-information. Also, hydrographs of the detector data are created. At the moment these are only the speed information, as they are important for travel time calculation on the edges. The service is not a webservice, so you can not ask.

### Build it

Build it as a Runnable JAR File


### Run it

Run the JAR with the following.

<pre>java -jar TravelTimeDailyCurveService.jar</pre>


## Developers

Sebastian Pape (TUD)

## Contact

* sebastian.pape@tu-dresden.de

## Acknowledgement
The Monitoring Service has been realized within the ExCELL project funded by the Federal Ministry for Economic Affairs and Energy (BMWi) and German Aerospace Center (DLR) - agreement 01MD15001B.


## Disclaimer

THIS SOFTWARE IS PROVIDED "AS IS" AND "WITH ALL FAULTS." 
BHS MAKES NO REPRESENTATIONS OR WARRANTIES OF ANY KIND CONCERNING THE 
QUALITY, SAFETY OR SUITABILITY OF THE SKRIPTS, EITHER EXPRESSED OR 
IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED WARRANTIES OF 
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

IN NO EVENT WILL BHS BE LIABLE FOR ANY INDIRECT, PUNITIVE, SPECIAL, 
INCIDENTAL OR CONSEQUENTIAL DAMAGES HOWEVER THEY MAY ARISE AND EVEN IF 
BHS HAS BEEN PREVIOUSLY ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.
