import type { Card } from "../card";
import type { Series, SingleSeries } from "../dataset";

import { createMockCard } from "./card";
import type { MockDatasetOpts } from "./dataset";
import { createMockDataset } from "./dataset";

export const createMockSingleSeries = (
  cardOpts: Partial<Card>,
  dataOpts: MockDatasetOpts = {},
): SingleSeries => {
  return {
    card: createMockCard(cardOpts),
    ...createMockDataset(dataOpts),
  };
};

export const createMockSeries = (
  opts: { name: string }[] = [{ name: "Series" }],
): Series => {
  return opts.map((opt) => createMockSingleSeries({ name: opt.name }));
};
