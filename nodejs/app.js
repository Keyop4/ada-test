const sqlite3 = require("sqlite3").verbose()
const express = require("express")

const app = express()
app.use(express.json())

let db = new sqlite3.Database("../database.db")

// Return all messages
app.get("/messages", (req, res) => {
    db.all("select body from messages", {}, (err, rows_raw) => {
        rows = rows_raw.map(row => row.body)
        res.send(rows)
    })
})

// Search for answers
app.post("/search", (req, res) => {
    let query = req.body.query

    db.all(
        "select id, title from answers where title like $query",
        { $query: "%" + query + "%" },
        (err, rows_raw) => {
            res.status(200).send(rows_raw)
        }
    )
})

var server = app.listen(5000, () => {
    console.log("Express server listening on port " + server.address().port)
})
