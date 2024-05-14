DROP SCHEMA IF EXISTS {schema} CASCADE;

CREATE SCHEMA {schema};

-- Create the 'parties' table
CREATE TABLE IF NOT EXISTS {schema}.parties (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create the 'constituencies' table
CREATE TABLE IF NOT EXISTS {schema}.constituencies (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    state VARCHAR(255) NOT NULL
);

-- Create the 'candidates' table
CREATE TABLE IF NOT EXISTS {schema}.candidates (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'others')),
    age INT NOT NULL,
    photo_url VARCHAR(255),
    party_id VARCHAR(255) REFERENCES {schema}.parties(id),
    constituency_id VARCHAR(255) REFERENCES {schema}.constituencies(id)
);

-- Create the 'voters' table
CREATE TABLE IF NOT EXISTS {schema}.voters (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    gender VARCHAR(10) CHECK (gender IN ('male', 'female', 'others')),
    age INT NOT NULL,
    city VARCHAR(255),
    state VARCHAR(255),
    pincode VARCHAR(10),
    phone_number VARCHAR(10) CHECK (LENGTH(phone_number) = 10),
    constituency_id VARCHAR(255) REFERENCES {schema}.constituencies(id)
);

-- Create the 'votes' table
CREATE TABLE IF NOT EXISTS {schema}.votes (
    voter_id VARCHAR(255) REFERENCES {schema}.voters(id),
    candidate_id VARCHAR(255) REFERENCES {schema}.candidates(id),
    voting_time INT NOT NULL,
    PRIMARY KEY (voter_id, candidate_id)
);
