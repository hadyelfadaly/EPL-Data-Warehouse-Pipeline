CREATE TABLE Stadium_dim
(
	StadiumID INT PRIMARY KEY,
	StadiumName VARCHAR(50)
);
CREATE TABLE Team_dim
(
	TeamID INT PRIMARY KEY,
	TeamName VARCHAR(50),
	StadiumID INT FOREIGN KEY REFERENCES Stadium_dim (StadiumID)
);
CREATE TABLE Pos_dim
(
	PosID INT PRIMARY KEY,
	PosName VARCHAR(50)
);
CREATE TABLE player_dim
(
	PlayerID INT PRIMARY KEY,
	PlayerName VARCHAR(50),
	PrevTeam Varchar(50),
	CurrTeam Varchar(50),
	PrevPos Varchar(50),
	CurrPos Varchar(50),
	Valid_from Date,
	Valid_to Date,
	Flag BIT
);
CREATE TABLE Date_dim
(
	DateID DATE PRIMARY KEY,
	YEAR INT ,
	Month INT,
	Day INT
);
CREATE TABLE PlayerFact
(
    PlayerID INT NOT NULL FOREIGN KEY REFERENCES Player_dim(PlayerID),
    TeamID INT NOT NULL FOREIGN KEY REFERENCES Team_dim(TeamID),
    PosID INT NOT NULL FOREIGN KEY REFERENCES Pos_dim(PosID),

    Apearances INT NULL,     
    Goals INT NULL,
    GoalsPerApearance DECIMAL(10,4) NULL,

    Penalties INT NULL,
    PenaltiesRatio DECIMAL(10,4) NULL,

    CONSTRAINT PK_PlayerFact PRIMARY KEY (PlayerID, TeamID, PosID)
);
CREATE TABLE MatchFact
(
    MatchID INT NOT NULL,
    HomeTeamID  INT NOT NULL FOREIGN KEY REFERENCES Team_dim(TeamID) ,      
    AwayTeamID  INT NOT NULL FOREIGN KEY REFERENCES Team_dim(TeamID),
    DateID DATE NOT NULL FOREIGN KEY REFERENCES Date_dim(DateID),
    StadiumID INT NOT NULL FOREIGN KEY REFERENCES Stadium_dim(StadiumID),
     
    HomeGoalsScored  INT NULL,
    AwayGoalsConceded INT NULL,
	Result VARCHAR(50) NULL,

	CONSTRAINT PK_MatchFact PRIMARY KEY (MatchID, HomeTeamID, AwayTeamID)

);

