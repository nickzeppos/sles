# SLES (State Legislative Effectiveness Score)
## repo to hold sles related files while I work in connor's dropbox


SLES Procedure

1. Copy an estimation script for the state you're working on from `SLES/State Legislatures/Estimate LES/Estimation_Scripts into `Connor SLES/State Legislatures/Estimate LES/Estimation_Scripts` and replace the "AV" suffix with "NWZ"
2. Ensure all of the files used in the script are available locally.[^1] In the WY script, that includes:
	- Bill_Details for both years in the relevant term, which should already exist at `State Legislative Data/States/WY_Bill_Details...`
	- Commemorative bill data, which you will need to generate via the `Commem Bills/Code_Commem_Bills_By_Session.R` script, which should produce an output at `State Legislative Data/Commem Bills/WY_Commem_Bills...`
		- Right now the output here should be hand-inspected and some bills will have to be manually upgraded.
	- Substantive and Significant and bill data, which should already exist at `State Legislative Data/Significant Bills/Project_Vote_Smart_bills/WY/WY_SS_Bills_...`
	- Legislator metadata, which should be manually downloaded and placed in the `/State Legislative Data/Legiscan/WY` folder
3. Run the script, it should produce an output that should end up in the `State Legislatures/LES_By_State/WY` dir. Doesn't really look like it gets written there, so check up on where the output ends up being written and move it manually if need be.
	- Inspect this file. If there are any members with LES's of zero, they need to be noted in the `Estimate LES/Zero_LES_legislators_Coded.csv`, and the estimation script should be re-run.
4. Now we have to write a state-specific compile script, which we're going to locate in the `State Legislatures/Compile LES/Nick Compile` folder. This should be modeled on two examples-- `Compile LES/extract_all_LES.R`, which is written to do all states at once, and Connor's state-specific compile scripts, `LES for Website/CA Scores for Website.R` is CA, e.g..
	- Before running this, you will have to manually provide committee chair information, which should be googled and then added to sheet 2 of the `Compile LES/Committee Chairs.xlsx`.
	- This file also computes seniority and majority. Seniority is computed by comparison to previous terms, which are taken from a "master" list that's loaded in at the beginning of the file in the examples scripts. The master list the other state compile scripts are using is `Compile LES/sles_all_2025-06-05.csv`, which I'm also going to use. Majority is computed by counting
	- Majority is derived from data itself. If there are independent/3rd party members, they should be assigned a majority value based on the party they are most closely associated/caucused with.[^2]
5. If all the covariates have been successfully added, you go ahead and compute benchmark, expectations, and the file can be written. It should be placed in the `Nick Compile` folder for Connor's inspection.




[^1]: All of this work is on Dropbox, so scripts will encounter errors unless the necessary resources are available offline, which isn't the default consequence of a Dropbox sync.
[^2]: I don't actually see where Connor's state-specific compile scripts are doing independents, but I assume this is because there were no I's in any of the states here?  Previous versions of compile seemed to write independents to a csv to be hand-coded. I will probably inspect output beforehand and write code that automatically assigns the proper majority value.




#cel 
#sles
