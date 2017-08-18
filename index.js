"use strict";

const pg = require('pg').native;
const sqlFormat = require('./lib/sqlFormat');

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
        where += ` ${k} = :${k}`;
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


let DB = function ( config ,logger) {
    
    logger = logger || require('./lib/logger');
    
    if ( !(this instanceof DB)) return new BD( config );
    let client = new pg.Pool( config );

    let query = function ( sql , param ) {
        
        let { sqlstr, data } = sqlFormat(sql, param);
        
        logger.debug('========================execute sql=============================');
        logger.debug(sqlstr);
        logger.debug(data);

        let timer = Date.now();
        return client.query(sql, data).then( result => {
            logger.debug(JSON.stringify(result));
            logger.debug(`========================cost ${Date.now() - timer}ms=============================`);
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
    
    this.transaction = function() {
        return new Promise( ( resolve , reject ) => {
            

        });
    };
    
};










pool.on('error', function(error, client) {
    console.error(error, client)
    console.info('Exit process')
    process.exit(1)
});

pool.on('connect', function() {
    console.log('db connected');
});

module.exports = pool;


