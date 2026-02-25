-- Here is the ETL From the database to the data warehouse
-- We will do simple ETL SQL Statements for all dimensions except the Player_dim
-- as we will implement SCD Type 6 there it will need a special procedure

-- Step 1: ETL to the Team, position, stadium and date dimensions.

-- Insert unique stadiums into Stadium_dim (used team pos as id)
INSERT INTO [EPL Datawarehouse].[dbo].[Stadium_dim] (StadiumID, StadiumName)
VALUES
(1, 'Etihad Stadium'),                -- Manchester City
(2, 'Anfield'),                       -- Liverpool
(3, 'Stamford Bridge'),               -- Chelsea
(4, 'Tottenham Hotspur Stadium'),     -- Tottenham
(5, 'Emirates Stadium'),              -- Arsenal
(6, 'Old Trafford'),                  -- Manchester United
(7, 'London Stadium'),                -- West Ham
(8, 'King Power Stadium'),            -- Leicester City
(9, 'Amex Stadium'),                  -- Brighton
(10, 'Molineux Stadium'),             -- Wolves
(11, 'St. James'' Park'),             -- Newcastle
(12, 'Selhurst Park'),                -- Crystal Palace
(13, 'Brentford Community Stadium'),  -- Brentford
(14, 'Villa Park'),                   -- Aston Villa
(15, 'St Mary''s Stadium'),           -- Southampton
(16, 'Goodison Park'),                -- Everton
(17, 'Elland Road'),                  -- Leeds United
(18, 'Turf Moor'),                    -- Burnley
(19, 'Vicarage Road'),                -- Watford
(20, 'Carrow Road');                  -- Norwich City

-- insert team details (used team pos as id like the stadium)
INSERT INTO [EPL Datawarehouse].[dbo].Team_dim (TeamID, TeamName, StadiumID)
SELECT p.Pos, p.Team, s.StadiumID FROM [EPL 21-22 Database].[dbo].[points_table] AS p,
[EPL Datawarehouse].[dbo].[Stadium_dim] AS s 
WHERE p.Pos = s.StadiumID; -- making sure each stadium with its right team

-- insert position details (row num as id)
INSERT INTO [EPL Datawarehouse].dbo.Pos_dim (PosID, PosName)
SELECT 
    ROW_NUMBER() OVER (ORDER BY Position) AS PosID,
    Position AS PosName
FROM 
(
    SELECT DISTINCT Position
    FROM [EPL 21-22 Database].dbo.all_players_stats
) AS p;

-- date dimension ETL
INSERT INTO [EPL Datawarehouse].dbo.Date_dim (DateID, [Year], [Month], [Day])
SELECT [Date], YEAR([Date]), MONTH([Date]), DAY([Date]) 
FROM 
(
SELECT DISTINCT [Date] FROM [EPL 21-22 Database].dbo.[all_match_results]
) AS d

-- STEP 2: ETL Procedure For Player_dim to implement SCD Type 6
--first create staging table for player dimension to compare with the main table for any 
-- scd type 6 updates or inserts

CREATE TABLE Staging_PlayerDim
(
    PlayerID INT,
    PlayerName VARCHAR(50),
    TeamID INT,
    PosID INT
);

-- INTIAL LOAD TO THE PLAYER_DIM BEFORE IMPLEMENTING TYPE 6 SCD
WITH PlayerLatest AS (
    SELECT
        Player,
        Team,
        Position,
        Apearances,
        ROW_NUMBER() OVER (
            PARTITION BY Player
            ORDER BY Apearances DESC
        ) AS rn
    FROM [EPL 21-22 Database].dbo.all_players_stats
)
INSERT INTO [EPL Datawarehouse].dbo.Player_dim
(PlayerID, PlayerName, CurrTeam, CurrPos, Valid_from, Flag)
SELECT ROW_NUMBER() OVER (ORDER BY p.Player), p.Player, t.TeamID, pos.PosID, GETDATE(), 1
FROM PlayerLatest AS p
JOIN Team_dim AS t ON p.Team = t.TeamName
JOIN Pos_dim AS pos ON p.Position = pos.PosName
WHERE p.rn = 1; -- pick ONLY the most recent team

GO

-- THE ETL STORED PROCEDURE to implement scd type 6
CREATE OR ALTER PROCEDURE Load_PlayerDim
AS
BEGIN

    -- TRUNCATE STAGING TABLE
    TRUNCATE TABLE [EPL Datawarehouse].dbo.Staging_PlayerDim;

    -- INSERT LATEST DATA INTO STAGING FROM SOURCE DATABASE
    INSERT INTO Staging_PlayerDim (PlayerName, TeamID, PosID)
    SELECT p.Player, t.TeamID, pos.PosID
    FROM ( SELECT Player, Team, Position, ROW_NUMBER() OVER (
                PARTITION BY Player ORDER BY Apearances DESC) AS rn
        FROM [EPL 21-22 Database].dbo.all_players_stats
    ) p
    JOIN Team_dim AS t ON p.Team = t.TeamName
    JOIN Pos_dim AS pos ON p.Position = pos.PosName
    WHERE p.rn = 1; -- one record per player

    -- INSERT NEW PLAYERS (NOT IN DIMENSION YET) BY COMPARING STAGING TABLE WITH OUR TABLE
    INSERT INTO [EPL Datawarehouse].dbo.Player_dim
    (PlayerID, PlayerName, CurrTeam, CurrPos, Valid_from, Flag)
    SELECT (SELECT MAX(PlayerID) + ROW_NUMBER() OVER(ORDER BY s.PlayerName) FROM Player_dim) AS PlayerID,
	s.PlayerName, t.TeamID AS CurrTeam, pos.PosID AS CurrPos, GETDATE() AS Valid_from, 1 AS Flag
    FROM [EPL Datawarehouse].dbo.Staging_PlayerDim AS s
    LEFT JOIN [EPL Datawarehouse].dbo.Player_dim AS d
        ON d.PlayerName = s.PlayerName AND d.Flag = 1
    JOIN [EPL Datawarehouse].dbo.Team_dim AS t
        ON s.TeamID = t.TeamID
    JOIN [EPL Datawarehouse].dbo.Pos_dim AS pos
        ON s.PosID = pos.PosID
    WHERE d.PlayerName IS NULL;  -- means new player

    -- DETECT CHANGES (TEAM OR POSITION CHANGES) (1ST CTE FOR UPDATE)
    
    SELECT d.PlayerID, d.CurrTeam AS OldTeamID, s.TeamID AS NewTeamID, 
	d.CurrPos AS OldPosID, s.PosID AS NewPosID
	INTO Changes
    FROM [EPL Datawarehouse].dbo.Player_dim AS d
    JOIN [EPL Datawarehouse].dbo.Staging_PlayerDim AS s 
    ON d.PlayerName = s.PlayerName
    WHERE d.Flag = 1
	AND (
          d.CurrTeam != s.TeamID   -- Team changed
          OR d.CurrPos  != s.PosID    -- Position changed
    )
  
    -- CLOSE OLD ROWS (TYPE 2)
    UPDATE [EPL Datawarehouse].dbo.Player_dim
    SET Valid_to = GETDATE(),
        Flag = 0
    WHERE PlayerID IN (SELECT PlayerID FROM Changes)
      AND Flag = 1;

    -- INSERT NEW ROWS FOR CHANGED PLAYERS (TYPE 2 + TYPE 3)
    INSERT INTO [EPL Datawarehouse].dbo.Player_dim
    (PlayerID, PlayerName, PrevTeam, CurrTeam, PrevPos, CurrPos, Valid_from, Valid_to, Flag)
    SELECT player.PlayerID, player.PlayerName, td.TeamID AS PrevTeam,
        tn.TeamID AS CurrTeam, pd.PosID AS PrevPos, pn.PosID AS CurrPos,
        GETDATE() AS Valid_from, NULL AS Valid_to, 1 AS Flag
    FROM Changes AS c
    JOIN [EPL Datawarehouse].dbo.Player_dim AS player 
        ON c.PlayerID = player.PlayerID
    JOIN [EPL Datawarehouse].dbo.Team_dim AS td
        ON c.OldTeamID = td.TeamID
    JOIN [EPL Datawarehouse].dbo.Team_dim AS tn
        ON c.NewTeamID = tn.TeamID
    JOIN [EPL Datawarehouse].dbo.Pos_dim AS pd
        ON c.OldPosID = pd.PosID
    JOIN [EPL Datawarehouse].dbo.Pos_dim AS pn
        ON c.NewPosID = pn.PosID;

	DROP TABLE Changes;

END;

GO
