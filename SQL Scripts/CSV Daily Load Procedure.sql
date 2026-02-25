-- Actiavate database mail

--EXEC sp_configure 'show advanced options', 1;  
--RECONFIGURE;
--EXEC sp_configure 'Database Mail XPs', 1;
--RECONFIGURE;

-- Stored procedure to laod the data
CREATE OR ALTER PROCEDURE LoadDailyCSVData
AS
BEGIN

    SET NOCOUNT ON; -- to not print the rows affected messages

    BEGIN TRY
        -- load CSV into a staging table
        TRUNCATE TABLE StagingMatchResults;
		TRUNCATE TABLE StagingPlayerStats;
		TRUNCATE TABLE StagingPointsTable;

		-- using bulk insert to get data from csv files
        BULK INSERT StagingMatchResults
        FROM 'D:\Uni\Advanced Databases\Assignments\Assignmnet 1\all_match_results.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
            TABLOCK
        );
		BULK INSERT StagingPlayerStats
        FROM 'D:\Uni\Advanced Databases\Assignments\Assignmnet 1\all_players_stats.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
            TABLOCK

        );
		BULK INSERT StagingPointsTable
        FROM 'D:\Uni\Advanced Databases\Assignments\Assignmnet 1\points_table.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
			CODEPAGE = '65001',
            TABLOCK --When inserting this data, take a lock on the entire table, not on individual rows or pages.
        );
        -- insert only NEW data into main table

		-- matches results table
        INSERT INTO all_match_results ([Date], HomeTeam, Result, AwayTeam)
        SELECT s.[Date], s.HomeTeam, s.Result, s.AwayTeam
        FROM StagingMatchResults AS s
        LEFT JOIN all_match_results AS m ON s.Date = m.Date AND s.HomeTeam = m.HomeTeam 
		AND s.Result = m.Result AND s.AwayTeam = m.AwayTeam
        WHERE m.Date IS NULL;

		-- player stats table
		INSERT INTO all_players_stats 
		( Team, JerseyNo, Player, Position, Apearances, Substitutions, Goals, Penalties, 
		YellowCards, RedCards)
		SELECT s.Team, s.JerseyNo, s.Player, s.Position, s.Apearances, s.Substitutions, s.Goals,
		s.Penalties, s.YellowCards, s.RedCards
		FROM StagingPlayerStats AS s
		LEFT JOIN all_players_stats AS p
		ON s.Team = p.Team AND s.JerseyNo  = p.JerseyNo AND s.Player = p.Player
		WHERE p.Team IS NULL;

		INSERT INTO points_table (Pos, Team, Pld, W, D, L, GF, GA, GD, Pts)
		SELECT s.Pos, s.Team, s.Pld, s.W, s.D, s.L, s.GF, s.GA, s.GD, s.Pts 
		FROM StagingPointsTable AS s 
		LEFT JOIN points_table AS p 
		ON s.Team = p.Team 
		WHERE p.Team IS NULL;

        -- send success email
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'hadyelfadaly@gmail.com',
            @recipients = '20236113@stud.fci-cu.edu.eg',
            @subject = 'CSV Load Successful',
            @body = 'Daily CSV load completed successfully.';
    END TRY

    BEGIN CATCH

        DECLARE @Err NVARCHAR(MAX) = ERROR_MESSAGE();
		DECLARE @BodyMessage NVARCHAR(MAX); 
		SET @BodyMessage = 'Error occurred: ' + @Err;


        -- 4. Send failure email
        EXEC msdb.dbo.sp_send_dbmail
            @profile_name = 'hadyelfadaly@gmail.com',
            @recipients = '20236113@stud.fci-cu.edu.eg', 
            @subject = 'CSV Load FAILED',
            @body = @BodyMessage;

    END CATCH

END;


