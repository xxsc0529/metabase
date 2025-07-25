import userEvent from "@testing-library/user-event";
import { useState } from "react";

import { createMockMetadata } from "__support__/metadata";
import { setupFieldValuesEndpoint } from "__support__/server-mocks";
import { renderWithProviders, screen, within } from "__support__/ui";
import { checkNotNull } from "metabase/lib/types";
import * as Lib from "metabase-lib";
import { createQuery } from "metabase-lib/test-helpers";
import Question from "metabase-lib/v1/Question";
import type { GetFieldValuesResponse } from "metabase-types/api";
import {
  ORDERS,
  ORDERS_ID,
  PEOPLE,
  PEOPLE_SOURCE_VALUES,
  SAMPLE_DB_ID,
  createSampleDatabase,
} from "metabase-types/api/mocks/presets";

import { QuestionFiltersHeader } from "./QuestionFiltersHeader";

const metadata = createMockMetadata({
  databases: [createSampleDatabase()],
});

type SetupOpts = {
  query?: Lib.Query;
  fieldValues?: GetFieldValuesResponse;
  isExpanded?: boolean;
};

function setup({
  query: initialQuery = TEST_MULTISTAGE_QUERY,
  fieldValues,
  isExpanded = true,
}: SetupOpts = {}) {
  const onChange = jest.fn();

  if (fieldValues) {
    setupFieldValuesEndpoint(fieldValues);
  }

  function WrappedFilterHeader() {
    const [query, setQuery] = useState(initialQuery);

    const question = Question.create({
      metadata,
      type: "query",
      dataset_query: Lib.toLegacyQuery(query),
    });

    const handleQueryChange = (question: Question) => {
      const nextQuery = question.query();
      setQuery(nextQuery);
      onChange(nextQuery);
    };

    return (
      <div data-testid="TEST_CONTAINER">
        <QuestionFiltersHeader
          question={question}
          expanded={isExpanded}
          updateQuestion={handleQueryChange}
        />
      </div>
    );
  }

  renderWithProviders(<WrappedFilterHeader />);

  function getNextQuery() {
    const [nextQuery] = onChange.mock.lastCall;
    return nextQuery;
  }

  function getFilterColumnNameForStage(stageIndex: number) {
    const query = getNextQuery();
    const [filter] = Lib.filters(query, stageIndex);
    const parts = Lib.filterParts(query, stageIndex, filter);
    const column = checkNotNull(parts?.column);
    return Lib.displayInfo(query, stageIndex, column).longDisplayName;
  }

  return { getNextQuery, getFilterColumnNameForStage };
}

describe("QuestionFiltersHeader", () => {
  it("should not render if expanded is false", () => {
    setup({ isExpanded: false });
    expect(screen.queryByTestId("TEST_CONTAINER")).toBeEmptyDOMElement();
  });

  it("should render filters from all stages", () => {
    setup();
    expect(screen.getAllByTestId("filter-pill")).toHaveLength(3);
    expect(screen.getByText("Quantity is greater than 4")).toBeInTheDocument();
    expect(screen.getByText("Count is greater than 5")).toBeInTheDocument();
    expect(screen.getByText("User → Source is Organic")).toBeInTheDocument();
  });

  it("should update a filter on the last stage", async () => {
    const { getNextQuery, getFilterColumnNameForStage } = setup({
      fieldValues: PEOPLE_SOURCE_VALUES,
    });

    await userEvent.click(screen.getByText("User → Source is Organic"));
    await userEvent.click(await screen.findByLabelText("Filter operator"));
    await userEvent.click(await screen.findByText("Is empty"));
    await userEvent.click(screen.getByText("Update filter"));

    expect(screen.getByText("User → Source is empty")).toBeInTheDocument();
    expect(
      screen.queryByText("User → Source is Organic"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Count is greater than 5")).toBeInTheDocument();

    const query = getNextQuery();
    expect(Lib.filters(query, 0)).toHaveLength(1);
    expect(Lib.filters(query, 1)).toHaveLength(1);
    expect(Lib.filters(query, 2)).toHaveLength(1);

    const [nextFilter] = Lib.filters(query, 2);
    const nextFilterParts = Lib.stringFilterParts(query, 2, nextFilter);
    const nextColumnName = getFilterColumnNameForStage(2);
    expect(nextFilterParts).toMatchObject({
      operator: "is-empty",
      column: expect.anything(),
      values: [],
      options: {},
    });
    expect(nextColumnName).toBe("User → Source");
  });

  it("should update a filter on the previous stage", async () => {
    const { getNextQuery, getFilterColumnNameForStage } = setup();

    await userEvent.click(screen.getByText("Count is greater than 5"));
    await userEvent.type(
      await screen.findByDisplayValue("5"),
      "{backspace}110",
    );
    await userEvent.click(screen.getByText("Update filter"));

    expect(screen.getByText("Count is greater than 110")).toBeInTheDocument();
    expect(
      screen.queryByText("Count is greater than 5"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("User → Source is Organic")).toBeInTheDocument();

    const query = getNextQuery();
    expect(Lib.filters(query, 0)).toHaveLength(1);
    expect(Lib.filters(query, 1)).toHaveLength(1);
    expect(Lib.filters(query, 2)).toHaveLength(1);

    const [nextFilter] = Lib.filters(query, 1);
    const nextFilterParts = Lib.numberFilterParts(query, 1, nextFilter);
    const nextColumnName = getFilterColumnNameForStage(1);
    expect(nextFilterParts).toMatchObject({
      operator: ">",
      column: expect.anything(),
      values: [110],
    });
    expect(nextColumnName).toBe("Count");
  });

  it("should remove a filter from the last stage", async () => {
    const { getNextQuery } = setup();

    await userEvent.click(
      within(screen.getByText("User → Source is Organic")).getByLabelText(
        "Remove",
      ),
    );

    expect(
      screen.queryByText("User → Source is Organic"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("Count is greater than 5")).toBeInTheDocument();

    const query = getNextQuery();
    expect(Lib.stageCount(query)).toBe(2);
    expect(Lib.filters(query, 0)).toHaveLength(1);
    expect(Lib.filters(query, 1)).toHaveLength(1);
  });

  it("should remove a filter from the previous stage", async () => {
    const { getNextQuery } = setup();

    await userEvent.click(
      within(screen.getByText("Count is greater than 5")).getByLabelText(
        "Remove",
      ),
    );

    expect(
      screen.queryByText("Count is greater than 5"),
    ).not.toBeInTheDocument();
    expect(screen.getByText("User → Source is Organic")).toBeInTheDocument();

    const query = getNextQuery();
    expect(Lib.filters(query, 0)).toHaveLength(1);
    expect(Lib.filters(query, 1)).toHaveLength(0);
    expect(Lib.filters(query, 2)).toHaveLength(1);
  });
});

/**
 * Stage 0: Count of Orders by User.Source, filtered by Orders.Quantity > 4
 * Stage 1: Count by User.Source, filtered by Count > 5
 * Stage 2: Count by user.Source, filtered by User.Source = "Organic"
 */
const TEST_MULTISTAGE_QUERY = createQuery({
  metadata,
  query: {
    type: "query",
    database: SAMPLE_DB_ID,
    query: {
      filter: [
        "=",
        [
          "field",
          "PEOPLE__via__USER_ID__SOURCE",
          {
            "base-type": "type/Text",
          },
        ],
        "Organic",
      ],
      "source-query": {
        aggregation: [["count"]],
        breakout: [
          [
            "field",
            "PEOPLE__via__USER_ID__SOURCE",
            {
              "base-type": "type/Text",
            },
          ],
        ],
        filter: [
          ">",
          [
            "field",
            "count",
            {
              "base-type": "type/Integer",
            },
          ],
          5,
        ],
        "source-query": {
          "source-table": ORDERS_ID,
          aggregation: [["count"]],
          breakout: [
            [
              "field",
              PEOPLE.SOURCE,
              {
                "base-type": "type/Text",
                "source-field": ORDERS.USER_ID,
              },
            ],
          ],
          filter: [
            ">",
            [
              "field",
              ORDERS.QUANTITY,
              {
                "base-type": "type/Integer",
              },
            ],
            4,
          ],
        },
      },
    },
  },
});
