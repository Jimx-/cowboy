#ifndef _TAGTREE_COW_TREE_NODE_H_
#define _TAGTREE_COW_TREE_NODE_H_

#include "bptree/page.h"
#include "bptree/serializer.h"
#include "tagtree/tree/cow_tree.h"

namespace tagtree {

template <unsigned int N, typename K, typename V, typename KeySerializer,
          typename KeyComparator, typename KeyEq, typename ValueSerializer>
class BaseCOWNode {
public:
    using BaseNodeType = BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>;
    using TreeType =
        COWTree<N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>;

    BaseCOWNode(BaseCOWNode* parent, bptree::PageID pid, bool new_node,
                KeyComparator kcmp = KeyComparator{}, KeyEq keq = KeyEq{})
        : pid(pid), parent(parent), kcmp(kcmp), keq(keq), size(0),
          new_node(new_node)
    {}

    bptree::PageID get_pid() const { return pid; }
    void set_pid(bptree::PageID id) { pid = id; }
    virtual bool is_leaf() const { return false; }
    bool is_new_node() const { return new_node; }

    BaseCOWNode* get_parent() const { return parent; }
    void set_parent(BaseCOWNode* parent) { this->parent = parent; }
    size_t get_size() const { return size; }
    void set_size(size_t size) { this->size = size; }
    K get_high_key() const { return high_key; }

    virtual void serialize(uint8_t* buf, size_t size) const = 0;
    virtual void deserialize(const uint8_t* buf, size_t size) = 0;

    virtual std::pair<std::shared_ptr<BaseNodeType>,
                      std::shared_ptr<BaseNodeType>>
    insert_value(typename TreeType::Transaction& txn, const K& key,
                 const V& value, K& split_key) = 0;

    virtual void
    print(std::ostream& os,
          const std::string& padding = "") = 0; /* for debug purpose */

protected:
    size_t size;
    BaseCOWNode* parent;
    bptree::PageID pid;
    bool new_node;
    KeyComparator kcmp;
    KeyEq keq;
    K high_key;
};

template <unsigned int N, typename K, typename V, typename KeySerializer,
          typename KeyComparator, typename KeyEq, typename ValueSerializer>
class LeafCOWNode;

template <unsigned int N, typename K, typename V,
          typename KeySerializer = bptree::CopySerializer<K>,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>,
          typename ValueSerializer = bptree::CopySerializer<V>>
class InnerCOWNode : public BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                        KeyEq, ValueSerializer> {
public:
    friend class LeafCOWNode<N, K, V, KeySerializer, KeyComparator, KeyEq,
                             ValueSerializer>;
    using BaseNodeType = BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>;
    using TreeType =
        COWTree<N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>;
    friend TreeType;
    using SelfType = InnerCOWNode<N, K, V, KeySerializer, KeyComparator, KeyEq,
                                  ValueSerializer>;

    InnerCOWNode(TreeType* tree, BaseNodeType* parent,
                 bptree::PageID pid = bptree::Page::INVALID_PAGE_ID,
                 bool new_node = true, KeySerializer kser = KeySerializer{},
                 KeyComparator kcmp = KeyComparator{})
        : BaseNodeType(parent, pid, new_node), tree(tree), key_serializer(kser)
    {
        for (int i = 0; i < N + 1; i++) {
            child_pages[i] = bptree::Page::INVALID_PAGE_ID;
        }
    }

    virtual void serialize(uint8_t* buf, size_t size) const
    {
        /* | size | keys | child_pages | */
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)this->size;
        buf += sizeof(uint32_t);
        size -= sizeof(uint32_t);
        size_t nbytes = key_serializer.serialize(buf, size, &this->high_key,
                                                 (&this->high_key) + 1);
        buf += nbytes;
        size -= nbytes;
        nbytes = key_serializer.serialize(buf, size, keys.begin(), keys.end());
        buf += nbytes;
        size -= nbytes;
        ::memcpy(buf, child_pages.begin(), sizeof(bptree::PageID) * N);
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        size -= sizeof(uint32_t);
        size_t nbytes = key_serializer.deserialize(
            &this->high_key, (&this->high_key) + 1, buf, size);
        buf += nbytes;
        size -= nbytes;
        nbytes =
            key_serializer.deserialize(keys.begin(), keys.end(), buf, size);
        buf += nbytes;
        size -= nbytes;
        ::memcpy(child_pages.begin(), buf, sizeof(bptree::PageID) * N);
        for (auto&& p : child_cache) {
            p.reset();
        }
    }

    BaseNodeType* get_child(int idx)
    {
        if (child_cache[idx]) {
            /* child in cache */
            return child_cache[idx].get();
        }

        if (child_pages[idx] != bptree::Page::INVALID_PAGE_ID) {
            if (!child_cache[idx]) {
                child_cache[idx] = tree->read_node(this, child_pages[idx]);
            }

            return child_cache[idx].get();
        }

        return nullptr;
    }

    virtual std::pair<std::shared_ptr<BaseNodeType>,
                      std::shared_ptr<BaseNodeType>>
    insert_value(typename TreeType::Transaction& txn, const K& key,
                 const V& value, K& split_key)
    {
        std::shared_ptr<SelfType> new_node, right_sibling;
        SelfType* new_node_ptr = this;

        if (!this->is_new_node()) {
            new_node = clone(txn);
            new_node_ptr = new_node.get();
        }

        auto it =
            std::upper_bound(new_node_ptr->keys.begin(),
                             new_node_ptr->keys.begin() + new_node_ptr->size,
                             key, new_node_ptr->kcmp);

        int child_idx = it - new_node_ptr->keys.begin();
        auto* child = new_node_ptr->get_child(child_idx);

        std::shared_ptr<BaseNodeType> new_child, child_sibling;
        std::tie(new_child, child_sibling) =
            child->insert_value(txn, key, value, split_key);

        if (new_child) {
            new_node_ptr->child_pages[child_idx] = new_child->get_pid();
            new_node_ptr->child_cache[child_idx] = std::move(new_child);
        }

        if (!child_sibling) {
            /* child did not split, done */
            return std::make_pair(new_node, nullptr);
        }

        /* insert the new sibling into the new node */
        ::memmove(&new_node_ptr->keys[child_idx + 1],
                  &new_node_ptr->keys[child_idx],
                  (new_node_ptr->size - child_idx) * sizeof(K));
        ::memmove(&new_node_ptr->child_pages[child_idx + 2],
                  &new_node_ptr->child_pages[child_idx + 1],
                  (new_node_ptr->size - child_idx) * sizeof(bptree::PageID));
        for (size_t i = new_node_ptr->size; i > child_idx; i--) {
            new_node_ptr->child_cache[i + 1] =
                std::move(new_node_ptr->child_cache[i]);
        }

        new_node_ptr->keys[child_idx] = split_key;
        new_node_ptr->child_pages[child_idx + 1] = child_sibling->get_pid();
        new_node_ptr->child_cache[child_idx + 1] = std::move(child_sibling);
        new_node_ptr->size++;

        if (new_node_ptr->size == N) {
            right_sibling = txn.template create_node<SelfType>(this->parent);

            right_sibling->size = new_node_ptr->size - N / 2 - 1;

            ::memcpy(right_sibling->keys.begin(),
                     &new_node_ptr->keys[N / 2 + 1],
                     right_sibling->size * sizeof(K));
            ::memcpy(right_sibling->child_pages.begin(),
                     &new_node_ptr->child_pages[N / 2 + 1],
                     right_sibling->size * sizeof(V));

            for (size_t i = N / 2 + 1, j = 0; i <= new_node_ptr->size;
                 i++, j++) {
                right_sibling->child_cache[j] =
                    std::move(new_node_ptr->child_cache[i]);
                if (right_sibling->child_cache[j]) {
                    right_sibling->child_cache[j]->set_parent(
                        right_sibling.get());
                }
            }

            split_key = new_node_ptr->keys[N / 2];
            new_node_ptr->size = N / 2;

            auto* mid_child = new_node_ptr->get_child(new_node_ptr->size - 1);
            right_sibling->high_key = new_node_ptr->high_key;
            new_node_ptr->high_key = mid_child->get_high_key();
        }

        return std::make_pair(new_node, right_sibling);
    }

    std::shared_ptr<SelfType> clone(typename TreeType::Transaction& txn)
    {
        auto new_node = txn.template create_node<SelfType>(this->parent);

        new_node->size = this->size;
        new_node->high_key = this->high_key;
        new_node->keys = keys;
        new_node->child_pages = child_pages;
        std::copy(child_cache.begin(), child_cache.begin() + this->size,
                  new_node->child_cache.begin());

        return new_node;
    }

    virtual void print(std::ostream& os, const std::string& padding = "")
    {
        this->get_child(0)->print(os, padding + "    ");
        for (int i = 0; i < this->size; i++) {
            os << padding << keys[i] << std::endl;
            this->get_child(i + 1)->print(os, padding + "    ");
        }
    }

private:
    TreeType* tree;
    std::array<K, N> keys;
    std::array<bptree::PageID, N + 1> child_pages;
    std::array<std::shared_ptr<BaseNodeType>, N + 1> child_cache;
    KeySerializer key_serializer;
};

template <unsigned int N, typename K, typename V,
          typename KeySerializer = bptree::CopySerializer<K>,
          typename KeyComparator = std::less<K>,
          typename KeyEq = std::equal_to<K>,
          typename ValueSerializer = bptree::CopySerializer<V>>
class LeafCOWNode : public BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                       KeyEq, ValueSerializer> {
public:
    friend class InnerCOWNode<N, K, V, KeySerializer, KeyComparator, KeyEq,
                              ValueSerializer>;
    using BaseNodeType = BaseCOWNode<N, K, V, KeySerializer, KeyComparator,
                                     KeyEq, ValueSerializer>;
    using TreeType =
        COWTree<N, K, V, KeySerializer, KeyComparator, KeyEq, ValueSerializer>;
    friend TreeType;
    using SelfType = LeafCOWNode<N, K, V, KeySerializer, KeyComparator, KeyEq,
                                 ValueSerializer>;

    LeafCOWNode(TreeType* tree, BaseNodeType* parent,
                bptree::PageID pid = bptree::Page::INVALID_PAGE_ID,
                bool new_node = true, KeySerializer kser = KeySerializer{},
                KeyComparator kcmp = KeyComparator{},
                ValueSerializer vser = ValueSerializer{})
        : BaseNodeType(parent, pid, new_node, kcmp), tree(tree),
          key_serializer(kser), value_serializer(vser)
    {}

    virtual bool is_leaf() const { return true; }

    virtual void serialize(uint8_t* buf, size_t size) const
    {
        /* | size | keys | child_pages | */
        *reinterpret_cast<uint32_t*>(buf) = (uint32_t)this->size;
        buf += sizeof(uint32_t);
        size -= sizeof(uint32_t);
        size_t nbytes = key_serializer.serialize(buf, size, &this->high_key,
                                                 (&this->high_key) + 1);
        buf += nbytes;
        size -= nbytes;
        nbytes = key_serializer.serialize(buf, size, keys.begin(), keys.end());
        buf += nbytes;
        size -= nbytes;
        nbytes =
            value_serializer.serialize(buf, size, values.begin(), values.end());
    }
    virtual void deserialize(const uint8_t* buf, size_t size)
    {
        this->size = (size_t) * reinterpret_cast<const uint32_t*>(buf);
        buf += sizeof(uint32_t);
        size -= sizeof(uint32_t);
        size_t nbytes = key_serializer.deserialize(
            &this->high_key, (&this->high_key) + 1, buf, size);
        buf += nbytes;
        size -= nbytes;
        nbytes =
            key_serializer.deserialize(keys.begin(), keys.end(), buf, size);
        buf += nbytes;
        size -= nbytes;
        nbytes = value_serializer.deserialize(values.begin(), values.end(), buf,
                                              size);
    }

    virtual std::pair<std::shared_ptr<BaseNodeType>,
                      std::shared_ptr<BaseNodeType>>
    insert_value(typename TreeType::Transaction& txn, const K& key,
                 const V& value, K& split_key)
    {
        std::shared_ptr<SelfType> new_node, right_sibling;
        SelfType* new_node_ptr = this;

        if (!this->is_new_node()) {
            new_node = clone(txn);
            new_node_ptr = new_node.get();
        }

        auto it =
            std::upper_bound(new_node_ptr->keys.begin(),
                             new_node_ptr->keys.begin() + new_node_ptr->size,
                             key, new_node_ptr->kcmp);
        size_t pos = it - new_node_ptr->keys.begin();

        ::memmove(it + 1, it, (new_node_ptr->size - pos) * sizeof(K));
        ::memmove(&new_node_ptr->values[pos + 1], &new_node_ptr->values[pos],
                  (new_node_ptr->size - pos) * sizeof(V));

        new_node_ptr->keys[pos] = key;
        new_node_ptr->values[pos] = value;
        new_node_ptr->size++;
        new_node_ptr->high_key = keys[new_node_ptr->size - 1];

        if (new_node_ptr->size == N) {
            right_sibling = txn.template create_node<SelfType>(this->parent);

            right_sibling->size = new_node_ptr->size - N / 2;

            ::memcpy(right_sibling->keys.begin(), &new_node_ptr->keys[N / 2],
                     right_sibling->size * sizeof(K));
            ::memcpy(right_sibling->values.begin(),
                     &new_node_ptr->values[N / 2],
                     right_sibling->size * sizeof(V));

            split_key = new_node_ptr->keys[N / 2];
            new_node_ptr->size = N / 2;

            right_sibling->high_key = new_node_ptr->high_key;
            new_node_ptr->high_key = new_node_ptr->keys[this->size - 1];
        }

        return std::make_pair(new_node, right_sibling);
    }

    std::shared_ptr<SelfType> clone(typename TreeType::Transaction& txn)
    {
        auto new_node = txn.template create_node<SelfType>(this->parent);

        new_node->size = this->size;
        new_node->high_key = this->high_key;
        new_node->keys = keys;
        new_node->values = values;

        return new_node;
    }

    virtual void print(std::ostream& os, const std::string& padding = "")
    {
        os << padding << "Page ID: " << this->get_pid() << std::endl;
        os << padding << "High key: " << this->high_key << std::endl;

        for (int i = 0; i < this->size; i++) {
            os << padding << keys[i] << " -> " << values[i] << std::endl;
        }
    }

private:
    TreeType* tree;
    std::array<K, N> keys;
    std::array<V, N> values;
    KeySerializer key_serializer;
    ValueSerializer value_serializer;
};

} // namespace tagtree

#endif
