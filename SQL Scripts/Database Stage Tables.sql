--ALTER TABLE [dbo].[all_match_results]
--ALTER COLUMN [Date] Date;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN JerseyNo INT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN Apearances INT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN Substitutions INT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN Goals INT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN Penalties INT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN YellowCards FLOAT;

--ALTER TABLE [dbo].[all_players_stats]
--ALTER COLUMN RedCards FLOAT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN Pld INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN W INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN L INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN D INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN GF INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN GA INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN GD INT;

--ALTER TABLE [dbo].[points_table]
--ALTER COLUMN Pts INT;

--CREATE TABLE StagingMatchResults
--(
--[Date] DATE,
--[HomeTeam] VARCHAR(50),
--[Result] VARCHAR(50),
--[AwayTeam] VARCHAR(50)
--)

--CREATE TABLE StagingPlayerStats
--(
--    Team VARCHAR(50) NULL,
--    JerseyNo INT NULL,
--    Player VARCHAR(50) NULL,
--    Position VARCHAR(50) NULL,
--    Appearances INT NULL,
--    Substitutions INT NULL,
--    Goals INT NULL,
--    Penalties INT NULL,
--    YellowCards FLOAT NULL,
--    RedCards FLOAT NULL,
--)

--CREATE TABLE StagingPointsTable
--(
--    Pos VARCHAR(50) NULL,
--    Team VARCHAR(50) NULL,
--    Pld INT NULL,
--    W INT NULL,
--    D INT NULL,
--    L INT NULL,
--    GF INT NULL,
--    GA INT NULL,
--    GD INT NULL,
--    Pts INT NULL,
--)