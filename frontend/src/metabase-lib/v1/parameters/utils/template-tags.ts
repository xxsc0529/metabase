import _ from "underscore";

import type { ParameterWithTarget } from "metabase-lib/v1/parameters/types";
import { getTemplateTagFromTarget } from "metabase-lib/v1/parameters/utils/targets";
import type {
  Card,
  Parameter,
  ParameterTarget,
  ParameterValuesConfig,
  TemplateTag,
} from "metabase-types/api";

function getParameterType(tag: TemplateTag) {
  if (tag["widget-type"]) {
    return tag["widget-type"];
  }

  const { type } = tag;

  if (type === "date") {
    return "date/single";
  }
  // @ts-expect-error -- preserving preexisting incorrect types (for now)
  if (type === "string" || type === "text") {
    return "string/=";
  }
  if (type === "number") {
    return "number/=";
  }
  if (type === "boolean") {
    return "boolean/=";
  }
  if (type === "temporal-unit") {
    return "temporal-unit";
  }

  return "category";
}

function getParameterTarget(tag: TemplateTag): ParameterTarget {
  return tag.type === "dimension" || tag.type === "temporal-unit"
    ? ["dimension", ["template-tag", tag.name]]
    : ["variable", ["template-tag", tag.name]];
}

export function getTemplateTagParameter(
  tag: TemplateTag,
  config?: ParameterValuesConfig,
): ParameterWithTarget {
  const type = getParameterType(tag);
  const isTemporalUnit = type === "temporal-unit";

  return {
    id: tag.id,
    type,
    target: getParameterTarget(tag),
    name: tag["display-name"],
    slug: tag.name,
    default: tag.default,
    required: tag.required,
    options: tag.options,
    values_query_type: config?.values_query_type,
    values_source_type: config?.values_source_type,
    values_source_config: config?.values_source_config,
    ...(isTemporalUnit && {
      temporal_units: config?.temporal_units,
    }),
  };
}

// NOTE: this should mirror `template-tag-parameters` in src/metabase/queries/models/card.clj
// If this function moves you should update the comment that links to this one
export function getTemplateTagParameters(
  tags: TemplateTag[],
  parameters: Parameter[] = [],
): ParameterWithTarget[] {
  const parametersById = _.indexBy(parameters, "id");

  return tags
    .filter(
      (tag) =>
        tag.type != null &&
        tag.type !== "card" &&
        tag.type !== "snippet" &&
        ((tag.type !== "dimension" && tag.type !== "temporal-unit") ||
          tag.dimension != null ||
          (tag["widget-type"] && tag["widget-type"] !== "none")),
    )
    .map((tag) => getTemplateTagParameter(tag, parametersById[tag.id]));
}

export function getTemplateTags(card: Card): TemplateTag[] {
  return card?.dataset_query?.type === "native" &&
    card.dataset_query.native["template-tags"]
    ? Object.values(card.dataset_query.native["template-tags"])
    : [];
}

export function getParametersFromCard(
  card: Card,
): Parameter[] | ParameterWithTarget[] {
  if (!card) {
    return [];
  }

  if (card.parameters && !_.isEmpty(card.parameters)) {
    return card.parameters;
  }

  return getTemplateTagParametersFromCard(card);
}

export function getTemplateTagParametersFromCard(card: Card) {
  const tags = getTemplateTags(card);
  return getTemplateTagParameters(tags, card.parameters);
}

// when navigating from dashboard --> saved native question,
// we are given dashboard parameters and a map of dashboard parameter ids to parameter values
// we need to transform this into a map of template tag ids to parameter values
// so that we populate the template tags in the native editor
export function remapParameterValuesToTemplateTags(
  templateTags: TemplateTag[],
  dashboardParameters: ParameterWithTarget[],
  parameterValuesByDashboardParameterId: {
    [key: string]: any;
  },
) {
  const parameterValues: {
    [key: string]: any;
  } = {};
  const templateTagParametersByName = _.indexBy(templateTags, "name");

  dashboardParameters.forEach((dashboardParameter) => {
    const { target } = dashboardParameter;
    const tag = getTemplateTagFromTarget(target);

    if (tag != null && templateTagParametersByName[tag]) {
      const templateTagParameter = templateTagParametersByName[tag];
      const parameterValue =
        parameterValuesByDashboardParameterId[dashboardParameter.id];
      parameterValues[templateTagParameter.name] = parameterValue;
    }
  });

  return parameterValues;
}
