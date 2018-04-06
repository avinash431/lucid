/*
This is my first Pig script
*/

A = load 'build.xml' using PigStorage(); -- loading data
dump A; -- printing results to screen


