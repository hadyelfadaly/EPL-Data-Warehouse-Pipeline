--answering analytical questions we built our dwh around

--Is a team performing better away or at home?

SELECT t.TeamName, SUM(mf1.HomeGoalsScored) AS [Home Goals],
(SELECT SUM(mf2.AwayGoalsScored) FROM MatchFact AS mf2 WHERE t.TeamID = mf2.AwayTeamID) AS [Away Goals]
FROM Team_dim AS t INNER JOIN MatchFact AS mf1 ON t.TeamID = mf1.HomeTeamID 
GROUP BY TeamName, TeamID;

-- gives us result which team home and away goals to answer

--===========================================================================

--Is a team performing better at the start of the season or at its end?

WITH TeamMatches AS
(

SELECT HomeTeamID AS TeamID, HomeGoalsScored AS GoalsScored, DateID FROM MatchFact
UNION
SELECT AwayTeamID AS TeamID, AwayGoalsScored AS GoalsScored, DateID FROM MatchFact

)
SELECT t.TeamName, d.Month,
SUM(tm.GoalsScored)
AS GoalsPerTeamPerMonth
FROM Team_dim AS t INNER JOIN TeamMatches AS tm ON t.TeamID = tm.TeamID INNER JOIN
Date_dim AS d ON tm.DateID = d.DateID
GROUP BY d.Month, t.TeamName
ORDER BY d.Month, t.TeamName;

--this shows each team goals per month we can investigate any team we want through filtering in the 
--where clause


--===================================================================================================

--Best attacking player

SELECT pd.PlayerName, pf.Goals, pf.GoalsPerApearance
FROM player_dim as pd INNER JOIN PlayerFact AS pf ON pd.PlayerID = pf.PlayerID
ORDER BY pf.Goals DESC, pf.GoalsPerApearance DESC, pf.Penalties, pf.PenaltiesRatio;

--getting player ordered by goals and goals per appearence and lowering their order if their 
--penalties ratio is high

--==========================================================================================

--what is number of goals scored in each stadium

SELECT sd.StadiumName, SUM(mf.HomeGoalsScored) + SUM(mf.AwayGoalsScored) AS GoalsScored
FROM Stadium_dim AS sd INNER JOIN MatchFact AS mf ON sd.StadiumID = mf.StadiumID
GROUP BY sd.StadiumName
ORDER BY GoalsScored DESC;

--man city the title winner stadium is highest stadium with goals in it