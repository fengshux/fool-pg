"use strict";


module.exports = function ( sql, param) {
    let keyTest = /:(\w+)/;
    let key;
    let keyCount = 1;
    let data = [];
    while( key = sql.match(keyTest) ) {
        
        sql = sql.replace(key[0], `$${keyCount++}`);
        let value = param[key[1]];
        data.push(value);
    }
    return {sql, data};
};
