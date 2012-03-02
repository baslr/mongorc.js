conn = new Mongo();
db = conn.getDB('mr');
print('connected to mr');

m = function()
{
    emit(this.photoId, {count:1});
}

r = function(key, values)
{
    var result = {count:0};
    
    values.forEach(function(value)
    {
        result.count += value.count;
    });

    return result;
}

sc = function() // show collections
{
    out = db.getCollectionNames();
    
    out.forEach(function(value)
    {
        print(value);
    });
}

mach = function()
{
    var cursor = db.mr_20120228.find().sort({'value.count':-1});
    
    print(cursor.count());
    
    for( ; cursor.hasNext(); )
    {
        i = cursor.next();
    
        res = db.mr_20120301.findOne({_id:i._id});
        
        if(res)
        {
            res2 = res.value.count - i.value.count;    
            
            if(res2 != 0) // ab oder aufstieg
            {
                print('id:' + i._id + ' um ' + res2 + ' aufgestiegen');
                db.change_20120301.insert({'photoId':i._id, 'change':res2});
            }
        }
        else
        {
            db.change_20120301.insert({'photoId':i._id, 'change':0}); // 0 fuer das erste mal dabei
            print('id:' + i._id + ' first time');
        }
    }
}

process = function()
{
    cur = db.mr_20120301.find();
    
    for(; cur.hasNext();)
    {
        i = cur.getNext();
        
        res = db.mr_20120301.findOne({_id:i._id});
        
        a = {   photoId:i._id,
                absolute:i.count,
                change:[{
                    date    : '2012-03-01',
                    relative: 0
                        }]
            };
    } // for
}
