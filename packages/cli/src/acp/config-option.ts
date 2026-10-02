import type { SessionConfigOption } from "@agentclientprotocol/sdk"
import type { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Order } from "effect"
import { builtinCommands, findModel, type Catalog } from "./catalog"

export const DEFAULT_VARIANT_VALUE = "default"

export type Selection = {
  readonly model?: Model.Ref
  readonly modeID?: Agent.ID
}

export function currentModel(catalog: Catalog, selection: Selection) {
  return selection.model ?? catalog.defaultModel
}

export function configOptions(catalog: Catalog, selection: Selection): SessionConfigOption[] {
  const model = currentModel(catalog, selection)
  const variants = findModel(catalog.models, model)?.variants.map((variant) => variant.id) ?? []
  return [
    {
      id: "model",
      name: "Model",
      category: "model",
      type: "select",
      currentValue: `${model.providerID}/${model.id}`,
      options: catalog.models
        .toSorted((a, b) => Order.String(a.providerID, b.providerID) || a.name.localeCompare(b.name))
        .map((item) => ({ value: `${item.providerID}/${item.id}`, name: `${item.providerID}/${item.name}` })),
    },
    ...(variants.length > 0
      ? [
          {
            id: "effort",
            name: "Effort",
            description: "Available effort levels for this model",
            category: "thought_level",
            type: "select",
            currentValue: selectVariant(model.variant, variants),
            options: [...new Set([...variants, DEFAULT_VARIANT_VALUE])].map((variant) => ({
              value: variant,
              name: variant
                .split(/[_-]/)
                .map((part) => (part ? part.charAt(0).toUpperCase() + part.slice(1) : part))
                .join(" "),
            })),
          } satisfies SessionConfigOption,
        ]
      : []),
    {
      id: "mode",
      name: "Session Mode",
      category: "mode",
      type: "select",
      currentValue: selection.modeID ?? catalog.defaultModeID,
      options: catalog.modes.map((mode) => ({
        value: mode.id,
        name: mode.name,
        ...(mode.description ? { description: mode.description } : {}),
      })),
    },
  ]
}

export function availableCommands(catalog: Catalog) {
  return [
    ...catalog.commands.map((command) => ({ name: command.name, description: command.description ?? "" })),
    ...Array.from(builtinCommands, ([name, command]) => ({ name, description: command.description })),
  ]
}

export function parseModelSelection(value: string, catalog: Catalog): Model.Ref {
  const providerID = catalog.models
    .map((model) => model.providerID)
    .toSorted()
    .find((id) => value.startsWith(`${id}/`))
  if (!providerID) {
    const separator = value.indexOf("/")
    if (separator === -1) return { providerID: Provider.ID.make(value), id: Model.ID.make("") }
    return { providerID: Provider.ID.make(value.slice(0, separator)), id: Model.ID.make(value.slice(separator + 1)) }
  }
  const id = Model.ID.make(value.slice(providerID.length + 1))
  if (findModel(catalog.models, { providerID, id })) return { providerID, id }
  const separator = id.lastIndexOf("/")
  const baseID = Model.ID.make(separator === -1 ? id : id.slice(0, separator))
  const variant = separator === -1 ? undefined : id.slice(separator + 1)
  const model = findModel(catalog.models, { providerID, id: baseID })
  if (model && variant && model.variants.some((item) => item.id === variant))
    return { providerID, id: baseID, variant: Model.VariantID.make(variant) }
  return { providerID, id }
}

function selectVariant(variant: string | undefined, variants: readonly string[]) {
  if (!variant || variant === DEFAULT_VARIANT_VALUE) return DEFAULT_VARIANT_VALUE
  if (variants.includes(variant)) return variant
  if (variants.includes(DEFAULT_VARIANT_VALUE)) return DEFAULT_VARIANT_VALUE
  return variants[0] ?? DEFAULT_VARIANT_VALUE
}
