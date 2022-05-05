### Claim filtering and blocking

  - Filtered claims are removed from claim search results (`blockchain.claimtrie.search`), they can still be resolved (`blockchain.claimtrie.resolve`)
  - Blocked claims are not included in claim search results and cannot be resolved.

Claims that are either filtered or blocked are replaced with a corresponding error message that includes the censoring channel id in a result that would return them.

#### How to filter or block claims:
  1. Make a channel (using lbry-sdk) and include the claim id of the channel in `--filtering_channel_ids` or `--blocking_channel_ids` used by `scribe-hub` **and** `scribe-elastic-sync`, depending on which you want to use the channel for. To use both blocking and filtering, make one channel for each.
  2. Using lbry-sdk, repost the claim to be blocked or filtered using your corresponding channel. If you block/filter a claim id for a channel, it will block/filter all of the claims in the channel.

#### Defaults

The example docker-composes in the setup guide use the following defaults:

Filtering:
  - `lbry://@LBRY-TagAbuse#770bd7ecba84fd2f7607fb15aedd2b172c2e153f`
  - `lbry://@LBRY-UntaggedPorn#95e5db68a3101df19763f3a5182e4b12ba393ee8`

Blocking
  - `lbry://@LBRY-DMCA#dd687b357950f6f271999971f43c785e8067c3a9`
  - `lbry://@LBRY-DMCARedFlag#06871aa438032244202840ec59a469b303257cad`
  - `lbry://@LBRY-OtherUSIllegal#b4a2528f436eca1bf3bf3e10ff3f98c57bd6c4c6`
