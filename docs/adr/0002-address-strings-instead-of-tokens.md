# 2. address-strings-instead-of-tokens

Date: 2025-11-20

## Status

Accepted

## Context

Address cleaning and matching can be approached in multiple ways. One common method is to tokenise addresses into their component parts (e.g., house number, street name, city, postcode) and perform matching based on these tokens. However, this approach can introduce complexity and potential errors during the tokenisation process.

This is the approach that many existing address matching systems take and something documented in detail by the [AIMS team at the ONS](https://www.ons.gov.uk/methodology/methodologicalpublications/generalmethodology/onsworkingpaperseries/onsworkingpaperseriesno17usingdatasciencefortheaddressmatchingservice#address-parsing).

Tokenisation allows for more granular and strict matching rules, but it can also lead to issues when addresses are formatted inconsistently or contain errors. Additionally, tokenisation can be computationally intensive and may not always yield better matching results compared to more holistic approaches.

## Decision

Initially, we've decided to represent addresses as complete strings rather than tokenising them into individual components. Our focus is instead on feature engineering the code, identifying token frequencies and patterns directly from the full address strings. This approach both simplifies our processing pipeline, whilst also allowing for variations in address formatting to be more naturally handled.

Robin's blog covers this topic in more detail: [Building Accurate Address Matching Systems](https://www.robinlinacre.com/address_matching/#how-to-represent-the-address-data---a-data-driven-approach)
As summarised in this blog:
> A tempting first step is to attempt to parse the messy addresses semantically - e.g. splitting out flat number, house number, street name and so on.3
>
> This is appealing because it appears to enable powerful scoring rules, such as 'if the building number is different, the addresses do not match'.
>
> In practice, this approach suffers from a paradox: the hardest addresses to match often contain ambiguities which make them the hardest to parse, and the problem of parsing the address correctly collapses into needing to find the true address.4

By avoiding tokenisation, we also reduce the complexity of our address processing pipeline, making it easier to maintain and extend. Users need only supply raw address strings, without needing to worry about the intricacies of tokenisation.

## Consequences

Approaching address matching in this way precludes certain match logic being used that would rely on tokenised addresses, such as strict matching rules based on individual address components. Numeric tokens (e.g., house numbers) may be more challenging to compare directly within full address strings, potentially requiring more sophisticated string comparison techniques, making exact matching logic more complicated to implement.

We feel, however, that the benefits of simplicity and flexibility outweigh these potential drawbacks, especially given the variability in address formats and the challenges associated with tokenisation.
