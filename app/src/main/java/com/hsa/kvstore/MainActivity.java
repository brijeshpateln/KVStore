package com.hsa.kvstore;

import android.app.ListActivity;
import android.content.Context;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.BaseAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ListView;
import android.widget.TableRow;
import android.widget.TextView;

import com.kvdb.DB;
import com.kvdb.KVDBException;
import com.kvdb.connection.DBConnection;

/**
 * Created by Brijesh on 30/05/2016.
 */
public class MainActivity extends ListActivity{
    ListView mListView;
    EditText mQueryEditText;
    EditText mKeyEditText;
    EditText mValueEditText;
    Button mQueryBtn;
    Button mPutBtn;
    Button mGetBtn;
    MyAdapter myAdapter;
    String query;
    DB db;
    DBConnection c;
    String[][] mResult;
    @Override
    protected void onCreate(Bundle savedInstanceState){
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_main);
        mQueryEditText = (EditText) findViewById(R.id.queryEditText);
        mQueryBtn = (Button) findViewById(R.id.queryBtn);
        mKeyEditText = (EditText) findViewById(R.id.keyEditText);
        mValueEditText = (EditText) findViewById(R.id.valueEditText);
        mPutBtn = (Button) findViewById(R.id.putBtn);
        mGetBtn = (Button) findViewById(R.id.getBtn);
        myAdapter = new MyAdapter(this);
        mQueryBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                query = mQueryEditText.getText().toString();
                try {
                    mResult = c.executeQueryForResult(query,null);
                    myAdapter.setResult(mResult);
                } catch (KVDBException e) {
                    e.printStackTrace();
                }
            }
        });
        mGetBtn.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v) {
                String key = mKeyEditText.getText().toString();
                try {
                    mValueEditText.setText(c.get(key));
                } catch (KVDBException e) {
                    e.printStackTrace();
                }
            }
        });
        mPutBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String key = mKeyEditText.getText().toString();
                String val = mValueEditText.getText().toString();
                try {
                    c.put(key,val);
                    mResult = c.executeQueryForResult("select _key, _value from kvstore;",null);
                    myAdapter.setResult(mResult);
                } catch (KVDBException e) {
                    e.printStackTrace();
                }
            }
        });


        mListView = getListView();
        myAdapter = new MyAdapter(this);

        mListView.setAdapter(myAdapter);
        try {
            db = DB.open(getApplicationContext().getFilesDir().getAbsolutePath().substring(0));
            c = db.getConnection();
        } catch (KVDBException e) {
            e.printStackTrace();
        }
    }

    class MyAdapter extends BaseAdapter {
        String[][] input;
        Context mContext;
        LayoutInflater inflater;

        MyAdapter(Context ctx) {
            mContext = ctx;
            inflater = LayoutInflater.from(mContext);
        }
        public void setResult(String[][] inp){
            input = inp;
            notifyDataSetChanged();
        }

        @Override
        public int getCount() {
            return input==null ? 0 : input.length;
        }

        @Override
        public Object getItem(int position) {
            return input[position];
        }

        @Override
        public long getItemId(int position) {
            return position;
        }

        @Override
        public View getView(int position, View convertView, ViewGroup parent) {
            ViewHolder vh = null;
            if(convertView == null){
                vh = new ViewHolder();
                convertView = inflater.inflate(R.layout.item, parent, false);
                LinearLayout ll = (LinearLayout) convertView.findViewById(R.id.linearLayout);
                vh.ll = ll;
                convertView.setTag(vh);
            } else {
                vh = (ViewHolder) convertView.getTag();
            }
            vh.set(input[position]);
            vh.ll.removeAllViews();
            vh.ll.setWeightSum((float)input[position].length);
            TableRow.LayoutParams params = new TableRow.LayoutParams(
                    LinearLayout.LayoutParams.MATCH_PARENT, LinearLayout.LayoutParams.MATCH_PARENT, 1.0f);
            params.setMargins(2,0,2,0);
            for(int i = 0; i < input[position].length; i++){
                vh.col_values[i].setLayoutParams(params);
                vh.ll.addView(vh.col_values[i]);
            }
            return convertView;
        }

        class ViewHolder {
            int count;
            int length;
            LinearLayout ll;
            TextView[] col_values;
            void set(String[] row){
                if(length != row.length){
                    length = row.length;
                    col_values = new TextView[length];
                }
                for(int i = 0; i < length; i++){
                    if(col_values[i] == null) {
                        col_values[i] = new TextView(MainActivity.this);
                    }
                    col_values[i].setText(row[i]);
                }
            }
        }
    }
}
