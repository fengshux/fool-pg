"use strict";

const pg = require('pg').native;
const co = require('co');
const sqlFormat = require('./lib/sqlFormat');
const _ = require('lodash');


let isArray = function( arr ) {
    return Object.prototype.toString.call(arr) === '[object Array]';
};

let getWhere = function( obj ) {
    if( !obj) return '';
    let where = '';
    for(var k in obj) {
        if( k == 'orderBy' || k == 'static_where'
          || k == 'limit' || k == 'offset') continue;
        if( where != '' ) where += ' AND ';
        where += ` ${k} = :w_${k}`;
        obj[`w_${k}`] = obj[k];
    }
    if( obj.static_where != '') {
        if( where != '' ) where += ' AND ';
        where += 'static_where';
    }
    if( where != '') where = ' where ' + where;

    return where;
};

let getOrderBy = function( obj ) {
    obj = obj || {};
    if(!obj.orderBy) return '';
    if(isArray(obj.orderBy)) {
        return ` order by ${obj.orderby.join(' , ')}`;
    }
    return ` order by ${obj.orderBy}`;
};

let getUpdateColumn = function( obj ) {
    let column = '';
    for(var k in obj) {
        if( column != '' ) column += ' AND ';
        column+= ` ${k} = :u_${k}`;
        obj[`u_${k}`] = obj[k];
    }
    return column;
};


let getInsertColumn = function( obj ) {
    let column = '';
    let value = '';
    let keys = Object.keys(obj);
    return {
        column: keys.join(' , '),
        value: keys.map(x => `:${x}`).join(' , ')
    };
};


let SqlUtil = function (db, logger) {

    if ( !(this instanceof SqlUtil)) return new SqlUtil(db, logger );

    let query = function ( sql , param ) {
        
        let { sqlstr, data } = sqlFormat(sql, param);
        
        logger.debug('========================execute sql=============================');
        logger.debug(sqlstr);
        logger.debug(data);

        let timer = Date.now();
        return db.query(sql, data).then( result => {
            logger.debug(JSON.stringify(result));
            logger.debug(`========================cost ${Date.now() - timer}ms=============================`);
            if(sql.toLowerCase().includes('insert') && !sql.toLowerCase().includes('returning')){
                return result;
            }
            if(sql.toLowerCase().includes('update') && !sql.toLowerCase().includes('returning')){
                return result;
            }
            return result;
        });
    };


    this.query = function ( sql, param ){
        return query(sql, param).then(result => result.rows);
    };
    
    this.selectList = function ( tableName, where ) {
        let sql = `select * from ${tableName} ${getWhere(where)} ${getOrderBy( where )}`;
        return query(sql, where).then(result => result.rows);
    };

    this.selectOne = function ( tableName, where ) {
        let sql = `select * from ${tableName} ${getWhere(where)}`;
        return query(sql, where).then(result => result.rows).then(rows => {
            if( rows.length > 0 ) {
                return rows[0];
            }
            return null;
        });
    };

    this.selectPage = function ( tableName, where ) {
        let sql = `select * from 
                     ${tableName} 
                     ${getWhere(where)}
                     ${getOrderBy( where )}
                     limit ${where.limit || 20} offset ${where.offset || 0}
                  `;
        let countSql = `select count(*) as count from ${tableName} ${getWhere(where)}`;
        
        return Promise.all([
            query(sql, where).then(result => result.rows),
            query(countSql, where).then(result => result.rows)
        ]).then(( [ rows, count]) => {
            let total = count[0].count;
            return { rows, total};
        });
    };

    this.update = function ( tableName, param, where ) {
        let sql = `
             update ${tableName} set ${getUpdatecolumn(param)} ${getWhere(where)}
         `;
        return query(sql, _.extend({}, param, where) );
    };

    this.save = function ( tableName, param ) {
        let column = getInsertColumn(param);
        let sql = `
            insert into ${tableName} ( ${column.column} ) values ( ${column.value} )
        `;
        return query(sql, param);
    };

    this.delete = function ( tableName, where ) {
        let sql = `
            delete from ${tableName} ${getWhere(where)}
        `;
        return query(sql, where);
    }; 
    
};


let DB = function ( config ,logger) {
    
    logger = logger || require('./lib/logger');
    
    if ( !(this instanceof DB)) return new BD( config );
    let client = new pg.Pool( config );

    _.extend(this, new SqlUtil(client, logger));
    
    this.transaction = co.wrap(function *( ) {

        let transClient = yield pool.connect();
        yield transClient.query('BEGIN');
        
        let release = function() {
            transClient.release();
        };
        
        let transaction = {};
        _.extend(transaction, new SqlUtil(transClient, logger));

        transaction.commit = function(){
            return transClient.query('COMMIT').then(()=>{
                release();
                transClient = null;
            });
        };

        transaction.rollback = function() {
            return transClient.query('ROLLBACK').then(()=>{
                release();
                transClient = null;
            });;
        };

        setTimeout(function(){
            if( transClient !=null ){
                transClient.query('ROLLBACK').then(()=>{
                    release();
                });
            }
        }, config.timeout, 90000);
        
        return transaction;
    });

    client.on('error', function(error, client) {
        logger.error(error, client);
        logger.info('Exit process');
        process.exit(1);
    });

    client.on('connect', function() {
        logger.log('db connected');
    });
    
};

module.exports = DB;
