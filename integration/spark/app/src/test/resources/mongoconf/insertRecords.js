use events;

db.createCollection("events", {
    validator: {
        $jsonSchema: {
            bsonType: "object",
            required: ["name", "date", "location"],
            properties: {
                name: {
                    bsonType: "string",
                    description: "must be a string and is required"
                },
                date: {
                    bsonType: "date",
                    description: "must be a date and is required"
                },
                location: {
                    bsonType: "string",
                    description: "must be a string and is required"
                }
            }
        }
    }
});

db.events.insertOne({
    name: "name",
    date: new Date(),
    location: "location"
});