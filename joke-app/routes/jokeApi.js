const express = require('express');
const router = express.Router();
const mysql = require('mysql2');
const path = require('path')


// DB connection setup - using env vars
let conStr = {}
conStr.host = process.env.MYSQL_SERVICE || 'localhost'
conStr.user = process.env.MYSQL_USER || 'admin'
conStr.password = process.env.MYSQL_PASSWORD || 'admin'
conStr.database = process.env.MYSQL_DATABASE || 'jokes_db'
conStr.port = process.env.MYSQL_PORT_INTERNAL || 3306
const db = mysql.createConnection(conStr)


// New joke route - fetches from DB
// takes ?count
router.get('/joke/:type', async (req, res) => {

    if(!req.params.type) {
        return res.status(400).json({ error: 'no type parameter given' });
    }

    let type = req.params.type;
    let count = parseInt(req.query.count) || 1;  // if no query param, default to 1

    if (req.query.count) {
        count = Number(req.query.count); // ensure
    }

    let jokes = await getJokes(count, type); // db query 

    if (jokes.length > 0) {
        res.setHeader('Content-Type', 'application/JSON');
        res.json({ jokes: jokes });
    }
    else {
        res.status(404).json({ error: 'no jokes of this type were found' });
    }
})



// New Type
router.get('/types', (req, res) => {
    let sql = `SELECT * FROM tbl_type`
    db.query(sql, (err, results) => { // db request
        if (err) {
            return res.sendStatus(500) // 500 error
        }
        res.json({ types: results.map(result => result.type) }) // just return type string, not whole record
    })
})

// FUNCTIONS \\

let getJokes = async function (numJokes, type) {
    let jokes = []
    let selectedJokes = []
    let sql = ''

    if (!numJokes || numJokes < 1) numJokes = 1 

    sql = `
     SELECT tbl_jokes.id, tbl_jokes.setup, tbl_jokes.punchline, tbl_type.type 
     FROM tbl_jokes
     inner join tbl_type
     on tbl_jokes.type = tbl_type.id
     `
    
     // filter if needed
     if (type !== 'any') {  
        sql += ` where tbl_type.type = "${type}"`
        sql += ` ORDER BY RAND() LIMIT ${numJokes}` // random order

        jokes = await new Promise((resolve, reject) => { // promise based rather than mysql promise - should really change
            db.query(sql, (err, results) => {
                if (err) {
                    reject(err)
                    return []
                }
                resolve(results)
            })
        })
        return jokes
     }
     else { // if any, just get random jokes without filter
        sql += ` ORDER BY RAND() LIMIT ${numJokes}`
        jokes = await new Promise((resolve, reject) => { 
            db.query(sql, (err, results) => {
                if (err) {
                    reject(err)
                    return []
                }
                resolve(results)
            })
        })
        return jokes
     }  

}



    
module.exports = router;