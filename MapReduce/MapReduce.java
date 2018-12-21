sshssh123

map(k,v,ctx) {
ctx.write(ctx.numeroColone, <v, k>);
ctx.numeroColone++
}
reduce(k',list<v> liste){
liste.sort par k
}


public class CompositeWritable implements Writable {
	
	Map<numeroColone, Value>() map;

    public CompositeWritable() {}

    public CompositeWritable(int val1, float val2) {
        liste.insert(numeroColone, val1);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        val1 = in.readInt();
        val2 = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(val1);
        out.writeInt(val2);
    }

	/* Merge 2 key-values */
    public void merge(CompositeWritable other) {
		
		foreach(<numcolone, value> in other.map){
			this.map.insert(numcolone, value)
		}
			
    }

    @Override
    public String toString() {
        return this.val1 + "\t" + this.val2 + "\t" + this.val3;
    }
}


public void reduce(Text key, Iterable<CompositeWritable> values, Context ctx) throws IOException, InterruptedException{

    CompositeWritable out;

    for (CompositeWritable next : values)
    {
        out.merge(next);
    }

    ctx.write(key, out);
}


ssh mkuser@ns328109.ip-5-135-143.eu
pwd : ece123
sudo useradd amichel
sudo passwd amichel
exit

ssh amichel@ns328109.up-5-135-143-eu
POM file : hdfs dfs -get /res/pom.xml ./
WC : hdfs dfs -get /res/WordCount.java ./

yarn jar nomdufichier.jar nomClasscontenantMain [paramètres]

Fichier texte : /res/big.tx
CSV : /res/csv/tp1/small.csv

Reducers : dans le dossier hdfs