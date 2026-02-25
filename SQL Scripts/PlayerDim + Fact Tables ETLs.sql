BEGIN TRY

-- Excute load playe dimension stored procedure

EXEC Load_PlayerDim;

-- Player fact etl
--CTE to hold the fresh calculated totals from the source
WITH AggregatedStats AS
(
SELECT pd.PlayerID, td.TeamID, pos.PosID, SUM(p.Apearances) AS Apearances,
SUM(p.Goals) AS Goals, SUM(p.Penalties) AS Penalties
FROM [EPL 21-22 Database].dbo.all_players_stats AS p 
INNER JOIN [EPL Datawarehouse].[dbo].[Player_dim] AS pd ON p.Player = pd.PlayerName AND pd.Flag = 1
INNER JOIN [EPL Datawarehouse].[dbo].[Team_dim] AS td ON td.TeamID = pd.CurrTeam 
INNER JOIN [EPL Datawarehouse].[dbo].[Pos_dim] AS pos ON pos.PosID = pd.CurrPos
GROUP BY pd.PlayerID, td.TeamID, pos.PosID
)
--MERGE the fresh stats into Fact table
MERGE INTO [EPL Datawarehouse].[dbo].[PlayerFact] AS Target
USING AggregatedStats AS Source
ON Target.PlayerID = Source.PlayerID 
   AND Target.TeamID = Source.TeamID 
   AND Target.PosID = Source.PosID

--If the player is already in the Fact table, UPDATE their totals
WHEN MATCHED THEN
    UPDATE SET 
        Target.Apearances = Source.Apearances,
        Target.Goals = Source.Goals,
        Target.Penalties = Source.Penalties,
        Target.GoalsPerApearance = (CASE WHEN Source.Apearances = 0 THEN 0 ELSE Source.Goals * 1.0 / Source.Apearances END),
        Target.PenaltiesRatio = (CASE WHEN Source.Goals = 0 THEN 0 ELSE Source.Penalties * 1.0 / Source.Goals END)

--If this is a new player, INSERT them
WHEN NOT MATCHED BY TARGET THEN
INSERT(PlayerID, TeamID, PosID, Apearances, Goals, GoalsPerApearance,
Penalties, PenaltiesRatio) 
VALUES( source.PlayerID, source.TeamID, source.PosID, Source.Apearances, 
source.Goals, 
(CASE WHEN source.Apearances = 0 THEN 0 
ELSE source.Goals*1.0/source.Apearances END),
source.Penalties,(CASE WHEN source.Goals = 0 
THEN 0 ELSE source.Penalties*1.0/source.Goals END)
);

-- Match fact ETL

DECLARE @maxMatchId INT;
SELECT @maxMatchId = ISNULL(MAX(MatchId), 0) FROM [EPL Datawarehouse].dbo.MatchFact;

INSERT INTO [EPL Datawarehouse].dbo.MatchFact (MatchID, HomeTeamID, AwayTeamID, DateID, StadiumID
,Result, HomeGoalsScored, AwayGoalsScored)
SELECT @maxMatchId + ROW_NUMBER() OVER(ORDER BY m.Date) AS MatchID, td.TeamID, td2.TeamID, dt.DateID,
sd.StadiumID, m.Result, (CAST(SUBSTRING(m.Result, 1, 1) AS INT)) AS HomeGoalsScored, 
(CAST(SUBSTRING(m.Result, 3, 1) AS INT)) AS AwayGoalsScored
FROM [EPL 21-22 Database].dbo.all_match_results AS m 
INNER JOIN [EPL Datawarehouse].dbo.Team_dim AS td ON m.HomeTeam = td.TeamName
INNER JOIN [EPL Datawarehouse].dbo.Team_dim AS td2 ON m.AwayTeam = td2.TeamName
INNER JOIN [EPL Datawarehouse].dbo.Date_dim AS dt ON m.Date = dt.DateID
INNER JOIN [EPL Datawarehouse].dbo.Stadium_dim AS sd ON td.StadiumID = sd.StadiumID
WHERE NOT EXISTS (
    SELECT 1 
    FROM [EPL Datawarehouse].dbo.MatchFact AS existing_mf
    WHERE existing_mf.HomeTeamID = td.TeamID 
      AND existing_mf.AwayTeamID = td2.TeamID 
      AND existing_mf.DateID = dt.DateID
);

-- send success email
EXEC msdb.dbo.sp_send_dbmail
     @profile_name = 'hadyelfadaly@gmail.com',
     @recipients = '20236113@stud.fci-cu.edu.eg',
     @subject = 'ETL Successful',
     @body = 'Daily ETL completed successfully.';

END TRY

BEGIN CATCH

	DECLARE @Err NVARCHAR(MAX) = ERROR_MESSAGE();
	DECLARE @BodyMessage NVARCHAR(MAX); 
	SET @BodyMessage = 'Error occurred: ' + @Err;

     --Send failure email
     EXEC msdb.dbo.sp_send_dbmail
          @profile_name = 'hadyelfadaly@gmail.com',
          @recipients = '20236113@stud.fci-cu.edu.eg', 
          @subject = 'ETL FAILED',
          @body = @BodyMessage;

END CATCH