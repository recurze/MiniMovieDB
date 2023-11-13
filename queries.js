use MiniMovieDB;
show collections;


// wrap mongodb query to print execution information: time and #results
function runQuery(query, description, queryType="find") {
    var start = new Date();

    var result = query();
    if (queryType == "find") {
        var numResults = result.count();
        if (numResults < 20) {
            while (result.hasNext()) {
                printjson(result.next());
            }
        }
    } else if (queryType == "aggregate") {
        var numResults = result.length;
        if (numResults < 20) {
            printjson(result);
        }
    }

    var end = new Date();

    print("\nQuery " + description);
    print("Time: " + (end - start) + "ms, #results: " + numResults);
}

function createIndices() {
    db.shows.createIndex({"primaryTitle": 1});
    db.people.createIndex({"name": 1});
    db.user_events.createIndex({"userId": 1});
    db.user_events.createIndex({"eventType": 1});
    db.user_ratings.createIndex({"userId": 1});
    db.user_ratings.createIndex({"imdbId": 1});
}


createIndices();


runQuery(function() {
    return db.shows.insertOne({
        "_id": "tt51045104",
        "primaryTitle": "DSA5104",
        "titleType": "Lectures",
        "startYear": 2023,
    });
}, "1. Add new show DSA5104 (tt51045104) released in 2023.", "insert");


runQuery(function() {
    return db.shows.updateOne(
        {"_id": "tt51045104"},
        {
            $set: {
                "endYear": 2023,
            },
        }
    );
}, "2. Update end year for tt51045104 (title: DSA5104).", "update");


runQuery(function() {
    return db.shows.find({"_id": "tt51045104"});
}, "2. DSA5104.");


runQuery(function() {
    return db.shows.find(
        {
            "primaryTitle": {$regex: /pulp fiction/i},
        },
        {
            _id: 1,
            primaryTitle: 1,
            startYear: 1,
            plot: 1
        }
    );
}, "3. Find movies with 'pulp fiction' in the title.");


runQuery(function() {
    return db.shows.find(
        {
            "taglines": { $regex: /^May the Force be with you/ },
        },
        {
            _id: 1,
            primaryTitle: 1,
            startYear: 1,
            plot: 1
        }
    );
}, "4. Find movies with tagline \"May the Force be with you.\"")


runQuery(function() {
    return db.shows.aggregate([
        {
            $match: {
                "genres": {
                    $in: ["Adventure", "Thriller"]
                },
            }
        },
        {
            $lookup: {
                from: "people",
                localField: "actors.id",
                foreignField: "_id",
                as: "actors_info"
            }
        },
        {
            $match: {
                "actors_info.name": {
                    $in: ["Cillian Murphy", "Anne Hathaway"]
                },
            }
        },
        {
            $unwind: "$crew"
        },
        {
            $lookup: {
                from: "people",
                localField: "crew.id",
                foreignField: "_id",
                as: "crew_info"
            }
        },
        {
            $match: {
                "crew.category": "director",
                "crew_info.name": {
                    $in: ["Quentin Tarantino", "Christopher Nolan"]
                },
            }
        },
        {
            $project: {
                primaryTitle: 1,
                startYear: 1,
                plot: 1,
                genres: 1,
            }
        }
    ]).toArray();
}, "5. List shows with at least one of user's preference from each category", "aggregate");


runQuery(function() {
    return db.shows.aggregate([
        {
            $match: {
                parentId: {
                    $ne: null
                },
                seasonNumber: {
                    $ne: null
                },
                episodeNumber: {
                    $ne: null
                }
            }
        },
        {
            $group: {
                _id: "$parentId",
                count: {
                    $sum: 1
                }
            }
        },
        {
            $sort: {
                count: -1
            }
        },
        {
            $limit: 1
        },
        {
            $lookup: {
                from: "shows",
                localField: "_id",
                foreignField: "parentId",
                as: "episodes"
            }
        },
        {
            $unwind: "$episodes"
        },
        {
            $sort: {
                "episodes.seasonNumber": 1,
                "episodes.episodeNumber": 1,
            }
        },
        {
            $project: {
                _id: "$episodes._id",
                seasonNumber: "$episodes.seasonNumber",
                episodeNumber: "$episodes.episodeNumber",
                primaryTitle: "$episodes.primaryTitle"
            }
        }
    ]).toArray();
}, "6. Find all the episodes of the show with the greatest number of episodes where the episode primary title, season number and episode number are all known, in chronological order.", "aggregate");


runQuery(function() {
    return db.user_events.aggregate([
        {
            $match: {
                userId: 5104,
            }
        },
        {
            $group: {
                _id: null,
                // Assuming session id grows with time (right?)
                lastSessionId: {$max: "$sessionId"}
            }
        },
        {
            $lookup: {
                from: "user_events",
                localField: "lastSessionId",
                foreignField: "sessionId",
                as: "eventsInLastSession",
            }
        },
        {
            $unwind: "$eventsInLastSession"
        },
        {
            $replaceRoot: {
                newRoot: "$eventsInLastSession"
            }
        },
        {
            $sort: {
                timestamp: 1
            }
        },
        {
            $addFields: {
                isoDate: {
                    $toDate: {
                        $multiply: ["$timestamp", 1000]
                    }
                }
            }
        },
        {
            $project: {
                timestamp: 0
            }
        }
    ]).toArray();
}, "7. List all events of the last session of the user \"5104\" in chronological order.", "aggregate");


runQuery(function() {
    return db.user_ratings.aggregate([
        {
            $group: {
                _id: "$imdbId",
                avgRatings: {
                    $avg: "$rating"
                },
                numRatings: {
                    $sum: 1
                },
            }
        },
        {
            $match: {
                numRatings: {
                    $gt: 100
                }
            }
        },
        {
            $facet: {
                top10: [
                    { $sort: { avgRatings: -1 } },
                    { $limit: 10 },
                    {
                        $lookup: {
                            from: "shows",
                            localField: "_id",
                            foreignField: "_id",
                            as: "show_info"
                        }
                    },
                    { $unwind: "$show_info" },
                    {
                        $project: {
                            title: "$show_info.primaryTitle",
                            avgRatings: 1,
                            numRatings: 1,
                        }
                    }
                ],
                bottom10: [
                    { $sort: { avgRatings: 1 } },
                    { $limit: 10 },
                    {
                        $lookup: {
                            from: "shows",
                            localField: "_id",
                            foreignField: "_id",
                            as: "show_info"
                        }
                    },
                    { $unwind: "$show_info" },
                    {
                        $project: {
                            title: "$show_info.primaryTitle",
                            avgRatings: 1,
                            numRatings: 1,
                        }
                    }
                ],
            }
        },
    ]).toArray();
}, "8. List 10 best and 10 worst shows rated at least 100 times, sorted by average rating.", "aggregate");


runQuery(function() {
    return db.user_events.aggregate([
        {
            $match: {
                "timestamp": {
                    $gte: new Date("2010-01-01").getTime() / 1000,
                    $lte: new Date("2010-01-01").getTime() / 1000 + 24 * 7 * 3600,
                }
            }
        },
        {
            $group: {
                _id: {
                    userId: "$userId",
                    eventType: "$eventType"
                },
                count: {
                    $sum: 1
                },
                watchTime: {
                    $sum: {
                        $subtract: [{
                                $ifNull: ["$playbackEndTimestamp", "$timestamp"]
                            },
                            "$timestamp"
                        ]
                    }
                }
            }
        },
        {
            $group: {
                _id: "$_id.eventType",
                avgCount: {
                    $avg: "$count",
                },
                avgWatchTime: {
                    $avg: "$watchTime",
                }
            }
        },
        {
            $project: {
                avgCount: 1,
                avgWatchTimeInMinutes: {
                    $divide: ["$avgWatchTime", 60]
                },
            }
        },
    ]).toArray()
}, "9. Compute aggregate metrics in the week of 2010-01-01", "aggregate");


runQuery(function() {
    return db.user_events.aggregate([{
            $match: {
                "timestamp": {
                    $gte: new Date("2010-01-01").getTime() / 1000,
                    $lte: new Date("2010-01-01").getTime() / 1000 + 24 * 7 * 3600,
                },
                "eventType": "playback",
            }
        },
        {
            $addFields: {
                date: {
                    $dateToString: {
                        format: "%Y-%m-%d",
                        date: {
                            $toDate: {
                                $multiply: ["$timestamp", 1000]
                            }
                        }
                    }
                }
            }
        },
        {
            $lookup: {
                from: "shows",
                localField: "imdbId",
                foreignField: "_id",
                as: "show_info"
            }
        },
        {
            $unwind: "$show_info"
        },
        {
            $project: {
                date: 1,
                imdbId: 1,
                genres: "$show_info.genres"
            }
        },
        {
            $unwind: "$genres"
        },
        {
            $group: {
                _id: {
                    date: "$date",
                    genres: "$genres"
                },
                shows: {
                    $addToSet: "$imdbId"
                }
            }
        },
        {
            $project: {
                _id: 0,
                date: "$_id.date",
                genre: "$_id.genres",
                numShows: {
                    $size: "$shows"
                }
            }
        },
        {
            $sort: {
                date: 1,
                numShows: -1
            }
        }
    ]).toArray();
}, "10. Compute average number of distinct shows watched per day by genre in the week 2010-01-01", "aggregate");


// Does not take into account cross-hour playback
runQuery(function() {
    return db.user_events.aggregate([{
            $match: {
                "timestamp": {
                    $gte: new Date("2010-01-01").getTime() / 1000,
                    $lte: new Date("2010-01-01").getTime() / 1000 + 24 * 7 * 3600,
                },
                "eventType": "playback",
            }
        },
        {
            $addFields: {
                date: {
                    $dateToString: {
                        format: "%Y-%m-%d",
                        date: {
                            $toDate: {
                                $multiply: ["$timestamp", 1000]
                            }
                        }
                    }
                },
                hour: {
                    $dateToString: {
                        format: "%H",
                        date: {
                            $toDate: {
                                $multiply: ["$timestamp", 1000]
                            }
                        }
                    }
                }
            }
        },
        {
            $group: {
                _id: {
                    date: "$date",
                    hour: "$hour"
                },
                numPlaybackEvents: {
                    $sum: 1
                }
            }
        },
        {
            $group: {
                _id: "$_id.hour",
                avgNumPlaybackEvents: {
                    $avg: "$numPlaybackEvents"
                }
            }
        },
        {
            $sort: {
                avgNumPlaybackEvents: -1
            }
        },
        {
            $project: {
                _id: 0,
                hour: "$_id",
            }
        }
    ]).toArray();
}, "11. Compute average load (#playback events) by hour in the week of 2010-01-01", "aggregate");


// Takes a lot of time
runQuery(function() {
    return db.shows.aggregate([
        {
            $project: {
                _id: 1,
                actor1: "$actors.id",
            }
        },
        {
            $lookup: {
                from: "shows",
                localField: "_id",
                foreignField: "_id",
                as: "show_info"
            }
        },
        {
            $unwind: "$show_info"
        },
        {
            $project: {
                actor1: "$actor1",
                actor2: "$show_info.actors.id",
            }
        },
        {
            $unwind: "$actor1"
        },
        {
            $unwind: "$actor2"
        },
        {
            $match: {
                $expr: {
                    $lt: ["$actor1", "$actor2"]
                }
            }
        },
        {
            $group: {
                _id: {
                    actor1: "$actor1",
                    actor2: "$actor2",
                },
                numCollaborations: {
                    $sum: 1
                }
            }
        },
        {
            $sort: {
                numCollaborations: -1
            }
        },
        {
            $limit: 5
        },
        {
            $lookup: {
                from: "people",
                localField: "_id.actor1",
                foreignField: "_id",
                as: "actor1_info",
            }
        },
        {
            $unwind: "$actor1_info"
        },
        {
            $lookup: {
                from: "people",
                localField: "_id.actor2",
                foreignField: "_id",
                as: "actor2_info",
            }
        },
        {
            $unwind: "$actor2_info"
        },
        {
            $project: {
                _id: 0,
                numCollaborations: 1,
                actor1: "$actor1_info.name",
                actor2: "$actor2_info.name",
            }
        }
    ]).toArray();
}, "12. Find the top 5 pairs of actors with greatest number of collaborations", "aggregate");


runQuery(function() {
    return db.user_ratings.aggregate([
        {
            $group: {
                _id: "$imdbId",
                avgRatings: {
                    $avg: "$rating"
                },
                numRatings: {
                    $sum: 1
                },
            }
        },
        {
            $match: {
                numRatings: {
                    $gt: 100
                },
                avgRatings: {
                    $gt: 4
                }
            }
        },
        {
            $lookup: {
                from: "shows",
                localField: "_id",
                foreignField: "_id",
                as: "show_info"
            }
        },
        {
            $unwind: "$show_info"
        },
        {
            $match: {
                "show_info.budgetCurrency": {
                    $ne: null
                },
                "show_info.budgetAmount": {
                    $ne: null
                },
            }
        },
        {
            $group: {
                _id: "$show_info.budgetCurrency",
                avgBudget: {
                    $avg: "$show_info.budgetAmount"
                }
            }
        },
    ]).toArray();
}, "13. Compute average budget by currency for shows (numRatings > 100, avgRating > 4)", "aggregate");


runQuery(function() {
    return db.shows.aggregate([
        {
            $unwind: "$tags"
        },
        {
            $match: {
                "tags.relevance": {
                    $gt: 0.95
                }
            }
        },
        {
            $project: {
                _id: 0,
                "id": "$_id",
                "tag": "$tags.tag"
            }
        },
        {
            $out: "show_tags"
        }
    ]).toArray();
}, "temp collection", "aggregate");



runQuery(function() {
    return db.user_ratings.aggregate([
        {
            $match: {
                "userId": 5104,
                "rating": 5.0,
            }
        },
        {
            $lookup: {
                from: "show_tags",
                localField: "imdbId",
                foreignField: "id",
                as: "tag_info"
            }
        },
        {
            $unwind: "$tag_info"
        },
        {
            $lookup: {
                from: "show_tags",
                localField: "tag_info.tag",
                foreignField: "tag",
                as: "recommendation"
            }
        },
        {
            $unwind: "$recommendation"
        },
        {
            $match: {
                $expr: {
                    $ne: ["$imdbId", "$recommendation.id"]
                },
            }
        },
        {
            $group: {
                _id: "$recommendation.id",
                numTagMatches: {
                    $sum: 1
                }
            }
        },
        {
            $sort: {
                numTagMatches: -1
            }
        },
        {
            $lookup: {
                from: "shows",
                localField: "_id",
                foreignField: "_id",
                as: "recommended"
            }
        },
        {
            $project: {
                _id: 0,
                id: "$watched.primaryTitle",
                recommendation: "$recommended.primaryTitle",
                recommendation_plot: "$recommended.plot",
                recommendation_genre: "$recommended.genres",
            }
        },
    ]).toArray();
}, "14. Recommend shows to user \"5104\" based on number of tag (with > 0.95 relevance) matches with 5-rated shows", "aggregate");


runQuery(function() {
    return db.user_ratings.aggregate([{
            $match: {
                "userId": 5104,
                "rating": 5.0,
            }
        },
        {
            $lookup: {
                from: "user_ratings",
                localField: "imdbId",
                foreignField: "imdbId",
                as: "fellow_cultured_person"
            }
        },
        {
            $unwind: "$fellow_cultured_person"
        },
        {
            $match: {
                "fellow_cultured_person.rating": 5.0
            }
        },
        {
            $lookup: {
                from: "user_ratings",
                localField: "fellow_cultured_person.userId",
                foreignField: "userId",
                as: "recommended_shows"
            }
        },
        {
            $unwind: "$recommended_shows"
        },
        {
            $match: {
                "recommended_shows.rating": 5.0
            }
        },
        {
            $group: {
                _id: "$recommended_shows.imdbId",
                numSimilarUsersWatched: {
                    $sum: 1
                }
            }
        },
        {
            $sort: {
                numSimilarUsersWatched: -1
            }
        },
        {
            $lookup: {
                from: "shows",
                localField: "_id",
                foreignField: "_id",
                as: "recommended"
            }
        },
        {
            $project: {
                _id: 0,
                id: "$watched.primaryTitle",
                recommendation: "$recommended.primaryTitle",
                recommendation_plot: "$recommended.plot",
                recommendation_genre: "$recommended.genres"
            }
        }
    ]).toArray();
}, "15. Recommend shows to user \"5104\" based on number of watches by users with similar taste", "aggregate");
