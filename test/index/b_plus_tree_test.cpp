#include "index/b_plus_tree.h"

#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest)
{
    bool read_from_disk = 0;
    // Init engine
    DBStorageEngine engine(db_name);
    std::vector<Column *> columns = {
        new Column("int", TypeId::kTypeInt, 0, false, false),
    };
    Schema *table_schema = new Schema(columns);
    KeyManager KP(table_schema, 22);
    BPlusTree tree(0, engine.bpm_, KP);
    TreeFileManagers mgr("tree_");
    // Prepare data
    const int n = 500;
    vector<GenericKey *> keys;
    vector<GenericKey *> keys_copy;
    vector<GenericKey *> delete_seq;
    vector<RowId> values;
    map<GenericKey *, RowId> kv_map;
    if (read_from_disk)
    {
        std::ifstream key_in("keys", ios::binary);
        for (int i = 0; i < n; i++)
        {
            GenericKey *tmp = KP.InitKey();
            tmp->desirialize(key_in, KP.GetKeySize());
            keys.push_back(tmp);
        }
        std::ifstream value_in("values", ios::binary);
        boost::archive::binary_iarchive ia_value(value_in);
        for (int i = 0; i < n; i++)
        {
            RowId tmp;
            ia_value >> tmp;
            values.push_back(tmp);
        }
        std::ifstream delete_in("delete_seq", ios::binary);
        for (int i = 0; i < n; i++)
        {
            GenericKey *tmp = KP.InitKey();
            tmp->desirialize(delete_in, KP.GetKeySize());
            delete_seq.push_back(tmp);
        }
        keys_copy = keys;
        std:sort(keys_copy.begin(),keys_copy.end(),
        [](GenericKey *A, GenericKey *B)
        {
            return *((uint8_t *)A + 17) > *((uint8_t *)B + 17);
        });
    }
    else
    {
        remove("keys");
        remove("values");
        remove("delete_seq");
        for (int i = 0; i < n; i++)
        {
            GenericKey *key = KP.InitKey();
            std::vector<Field> fields{Field(TypeId::kTypeInt, i)};
            KP.SerializeFromKey(key, Row(fields), table_schema);
            keys.push_back(key);
            values.push_back(RowId(i));
            delete_seq.push_back(key);
        }
        keys_copy = keys;
        // Shuffle data
        ShuffleArray(keys);
        ShuffleArray(values);
        ShuffleArray(delete_seq);
        //! save data to disk
        /*
        std::ofstream key_out;
        key_out.rdbuf()->pubsetbuf(0, 0); // disable buffering
        key_out.open("keys", ios::binary);
        for (int i = 0; i < n; i++)
            keys[i]->serialize(key_out, KP.GetKeySize());
        std::ofstream value_out("values", ios::binary);
        boost::archive::binary_oarchive oa(value_out);
        for (int i = 0; i < n; i++)
            oa &values[i];
        std::ofstream delete_out;
        delete_out.rdbuf()->pubsetbuf(0, 0); // disable buffering
        delete_out.open("delete_seq", ios::binary);
        for (int i = 0; i < n; i++)
            delete_seq[i]->serialize(delete_out, KP.GetKeySize());
        */
    }
    // Map key value
    for (int i = 0; i < n; i++)
    {
        kv_map[keys[i]] = values[i];
    }

    // Insert data
    for (int i = 0; i < n; i++)
    {
        tree.Insert(keys[i], values[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Print tree
    tree.PrintTree(mgr[0]);
    // Search keys
    vector<RowId> ans;
    for (int i = 0; i < n; i++)
    {
        tree.GetValue(keys_copy[i], ans);
        ASSERT_EQ(kv_map[keys_copy[i]], ans[i]);
    }
    ASSERT_TRUE(tree.Check());
    // Delete half keys
    for (int i = 0; i < n / 2; i++)
    {
        // LOG(INFO) << "delete " << (int)*((unsigned char *)delete_seq[i] + 17);
        tree.Remove(delete_seq[i]);
    }
    tree.PrintTree(mgr[1]);
    // Check valid
    ans.clear();
    for (int i = 0; i < n / 2; i++)
    {
        // LOG(INFO) << "looking for deleted " << (int)*((unsigned char *)delete_seq[i] + 17);
        ASSERT_FALSE(tree.GetValue(delete_seq[i], ans));
    }
    for (int i = n / 2; i < n; i++)
    {
        // LOG(INFO) << "looking for " << (int)*((unsigned char *)delete_seq[i] + 17);
        // if (!tree.GetValue(delete_seq[i], ans))
        //     LOG(WARNING) << "found bug";
        ASSERT_TRUE(tree.GetValue(delete_seq[i], ans));
        ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
    }
    ASSERT_TRUE(tree.Check());
}