# HPSS Integrity Crawler
----
<b>Tom Barron</b>
tbarron@ornl.gov

> This work used the resources of the Oak Ridge Leadership Computing
> Facility, located in the National Center for Computational Sciences at
> the Oak Ridge National Laboratory, which is managed by UT Battelle,
> LLC for the U.S. Department of Energy, under contract No.
> DEAC05-00OR22725.

## Problem Addressed

HPSS is a large, complex, long-term project storing petabytes of data
accummulated over a period of decades. Given the volume of data
stored, it can be difficult to know what is stored and whether it
remains accurate and self-consistent.

## Audience

The immediate audience for HPSSIC comprises the administrators who
need to be aware of any discrepancies or errors that appear in the
HPSS archive metadata in order to address them. The ultimate audience
includes all HPSS users since the goal is to provide long term
confidence that HPSS is reliable and the data in it is correct and
complete.

## Solution

HPSSIC (HPSS Integrity Crawler) is an attempt to provide a level of
confidence that the data stored in HPSS has not changed since it was
stored, that it is complete and accurate, and that discrepancies in
the HPSS archive metadata are identified and addressed.

## Team

HPSSIC was written by Tom Barron with input from Vicky White, Deryl
Steinert, and Mitch Griffith under the direction of Sudharshan
Vazhkudai. HPSSIC is deployed and operated at ORNL by Jason Anderson,
Quinn Mitchell, and Stan White.

## Metrics for Success

OLCF users rely on HPSS with confidence.

Issues that might affect the integrity of the archive are identified
and addressed early.


